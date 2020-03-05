# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Functions to perform the XE installation
#
# written by Andrew Peace

import os
import os.path
import subprocess
import datetime
import re
import tempfile

import repository
import generalui
import xelogging
import util
import diskutil
from disktools import *
import fcoeutil
import netutil
import shutil
import constants
import hardware
import upgrade
import init_constants
import scripts
import xcp.bootloader as bootloader
import netinterface
import tui.repo
import xcp.dom0
from xcp import logger
from xcp.version import Version

# Product version and constants:
import version
from version import *
from constants import *
from diskutil import getRemovableDeviceList
from uicontroller import REPEAT_STEP

MY_PRODUCT_BRAND = PRODUCT_BRAND or PLATFORM_NAME

class InvalidInstallerConfiguration(Exception):
    pass

################################################################################
# FIRST STAGE INSTALLATION:

class Task:
    """
    Represents an install step.
    'fn'   is the function to execute
    'args' is a list of value labels identifying arguments to the function,
    'returns' is a list of the labels of the return values, or a function
           that, when given the 'args' labels list, returns the list of the
           labels of the return values.
    """

    def __init__(self, fn, args, returns, args_sensitive=False,
                 progress_scale=1, pass_progress_callback=False,
                 progress_text=None):
        self.fn = fn
        self.args = args
        self.returns = returns
        self.args_sensitive = args_sensitive
        self.progress_scale = progress_scale
        self.pass_progress_callback = pass_progress_callback
        self.progress_text = progress_text

    def execute(self, answers, progress_callback=lambda x: ()):
        args = self.args(answers)
        assert type(args) == list

        if not self.args_sensitive:
            logger.log("TASK: Evaluating %s%s" % (self.fn, args))
        else:
            logger.log("TASK: Evaluating %s (sensitive data in arguments: not logging)" % self.fn)

        if self.pass_progress_callback:
            args.insert(0, progress_callback)

        rv = apply(self.fn, args)
        if type(rv) is not tuple:
            rv = (rv,)
        myrv = {}

        if callable(self.returns):
            ret = apply(self.returns, args)
        else:
            ret = self.returns

        for r in range(len(ret)):
            myrv[ret[r]] = rv[r]
        return myrv

###
# INSTALL SEQUENCES:
# convenience functions
# A: For each label in params, gives an arg function that evaluates
#    the labels when the function is called (late-binding)
# As: As above but evaluated immediately (early-binding)
# Use A when you require state values as well as the initial input values
A = lambda ans, *params: ( lambda a: [a.get(param) for param in params] )
As = lambda ans, *params: ( lambda _: [ans.get(param) for param in params] )

def getPrepSequence(ans, interactive):
    seq = [
        Task(util.getUUID, As(ans), ['installation-uuid']),
        Task(util.getUUID, As(ans), ['control-domain-uuid']),
        Task(util.randomLabelStr, As(ans), ['disk-label-suffix']),
        Task(diskutil.create_raid, A(ans, 'raid'), []),
        Task(inspectTargetDisk, A(ans, 'primary-disk', 'installation-to-overwrite', 'initial-partitions', 'preserve-first-partition', 'sr-on-primary', 'create-new-partitions'), ['target-boot-mode', 'boot-partnum', 'primary-partnum', 'backup-partnum', 'logs-partnum', 'swap-partnum', 'storage-partnum']),
        Task(selectPartitionTableType, A(ans, 'primary-disk', 'install-type', 'primary-partnum', 'create-new-partitions'), ['partition-table-type']),
        ]

    if ans['time-config-method'] == 'ntp':
        seq.append(Task(setTimeNTP, A(ans, 'ntp-servers'), []))
    elif ans['time-config-method'] == 'manual':
        seq.append(Task(setTimeManually, A(ans, 'localtime', 'set-time-dialog-dismissed', 'timezone'), []))

    if not interactive:
        seq.append(Task(verifyRepos, A(ans, 'sources', 'ui'), []))
    if ans['install-type'] == INSTALL_TYPE_FRESH:
        seq += [
            Task(removeBlockingVGs, As(ans, 'guest-disks'), []),
            Task(writeDom0DiskPartitions, A(ans, 'primary-disk', 'target-boot-mode', 'boot-partnum', 'primary-partnum', 'backup-partnum', 'logs-partnum', 'swap-partnum', 'storage-partnum', 'sr-at-end', 'partition-table-type', 'create-new-partitions', 'new-partition-layout'), ['new-partition-layout']),
            ]
        seq.append(Task(writeGuestDiskPartitions, A(ans,'primary-disk', 'guest-disks', 'partition-table-type'), []))
    elif ans['install-type'] == INSTALL_TYPE_REINSTALL:
        seq.append(Task(getUpgrader, A(ans, 'installation-to-overwrite'), ['upgrader']))
        if 'backup-existing-installation' in ans and ans['backup-existing-installation']:
            seq.append(Task(doBackup,
                            lambda a: [ a['upgrader'] ] + [ a[x] for x in a['upgrader'].doBackupArgs ],
                            lambda progress_callback, upgrader, *a: upgrader.doBackupStateChanges,
                            progress_text="Backing up existing installation...",
                            progress_scale=100,
                            pass_progress_callback=True))
        seq.append(Task(prepareTarget,
                        lambda a: [ a['upgrader'] ] + [ a[x] for x in a['upgrader'].prepTargetArgs ],
                        lambda progress_callback, upgrader, *a: upgrader.prepTargetStateChanges,
                        progress_text="Preparing target disk...",
                        progress_scale=100,
                        pass_progress_callback=True))
        seq.append(Task(prepareUpgrade,
                        lambda a: [ a['upgrader'] ] + [ a[x] for x in a['upgrader'].prepUpgradeArgs ],
                        lambda progress_callback, upgrader, *a: upgrader.prepStateChanges,
                        progress_text="Preparing for upgrade...",
                        progress_scale=100,
                        pass_progress_callback=True))
    seq += [
        Task(createDom0DiskFilesystems, A(ans, 'install-type', 'primary-disk', 'target-boot-mode', 'boot-partnum', 'primary-partnum', 'logs-partnum', 'disk-label-suffix'), []),
        Task(updateBootLoaderLocation, A(ans, 'target-boot-mode', 'partition-table-type', 'primary-disk', 'bootloader-location'), ['bootloader-location']),
        Task(mountVolumes, A(ans, 'primary-disk', 'boot-partnum', 'primary-partnum', 'logs-partnum', 'cleanup', 'target-boot-mode'), ['mounts', 'cleanup']),
        ]
    return seq

def getRepoSequence(ans, repos):
    seq = []
    for repo in repos:
        seq.append(Task(repo.installPackages, A(ans, 'mounts', 'kernel-alt'), [],
                     progress_scale=100,
                     pass_progress_callback=True,
                     progress_text="Installing %s..." % repo.name()))
        seq.append(Task(repo.record_install, A(ans, 'mounts', 'installed-repos'), ['installed-repos']))
        seq.append(Task(repo.getBranding, A(ans, 'mounts', 'branding'), ['branding']))
    return seq

def getFinalisationSequence(ans):
    seq = [
        Task(importYumAndRpmGpgKeys, A(ans, 'mounts'), []),
        Task(writeResolvConf, A(ans, 'mounts', 'manual-hostname', 'manual-nameservers'), []),
        Task(writeMachineID, A(ans, 'mounts'), []),
        Task(writeKeyboardConfiguration, A(ans, 'mounts', 'keymap'), []),
        Task(configureNetworking, A(ans, 'mounts', 'net-admin-interface', 'net-admin-bridge', 'net-admin-configuration', 'manual-hostname', 'manual-nameservers', 'network-hardware', 'preserve-settings', 'network-backend'), []),
        Task(prepareSwapfile, A(ans, 'mounts', 'primary-disk', 'swap-partnum', 'disk-label-suffix'), []),
        Task(writeFstab, A(ans, 'mounts', 'target-boot-mode', 'primary-disk', 'logs-partnum', 'swap-partnum', 'disk-label-suffix'), []),
        Task(enableAgent, A(ans, 'mounts', 'network-backend', 'services'), []),
        Task(writeInventory, A(ans, 'installation-uuid', 'control-domain-uuid', 'mounts', 'primary-disk',
                               'backup-partnum', 'storage-partnum', 'guest-disks', 'net-admin-bridge',
                               'branding', 'net-admin-configuration', 'host-config', 'new-partition-layout', 'partition-table-type', 'install-type'), []),
        Task(writeXencommons, A(ans, 'control-domain-uuid', 'mounts'), []),
        Task(configureISCSI, A(ans, 'mounts', 'primary-disk'), []),
        Task(mkinitrd, A(ans, 'mounts', 'primary-disk', 'primary-partnum',
                              'fcoe-interfaces'), []),
        Task(prepFallback, A(ans, 'mounts', 'primary-disk', 'primary-partnum'), []),
        Task(installBootLoader, A(ans, 'mounts', 'primary-disk', 'partition-table-type',
                                  'boot-partnum', 'primary-partnum', 'target-boot-mode', 'branding',
                                  'disk-label-suffix', 'bootloader-location', 'write-boot-entry', 'install-type',
                                  'serial-console', 'boot-serial', 'host-config', 'fcoe-interfaces'), []),
        Task(postInstallAltKernel, A(ans, 'mounts', 'kernel-alt'), []),
        Task(touchSshAuthorizedKeys, A(ans, 'mounts'), []),
        Task(setRootPassword, A(ans, 'mounts', 'root-password'), [], args_sensitive=True),
        Task(setTimeZone, A(ans, 'mounts', 'timezone'), []),
        Task(writei18n, A(ans, 'mounts'), []),
        Task(configureMCELog, A(ans, 'mounts'), []),
        ]

    # on fresh installs, prepare the storage repository as required:
    if ans['install-type'] == INSTALL_TYPE_FRESH:
        seq += [
            Task(prepareStorageRepositories, A(ans, 'mounts', 'primary-disk', 'storage-partnum', 'guest-disks', 'sr-type'), []),
            Task(configureSRMultipathing, A(ans, 'mounts', 'primary-disk'), []),
            ]
    if ans['time-config-method'] == 'ntp':
        seq.append(Task(configureNTP, A(ans, 'mounts', 'ntp-servers'), []))
    # complete upgrade if appropriate:
    if ans['install-type'] == constants.INSTALL_TYPE_REINSTALL:
        seq.append( Task(completeUpgrade, lambda a: [ a['upgrader'] ] + [ a[x] for x in a['upgrader'].completeUpgradeArgs ], []) )

    # run the users's scripts
    seq.append( Task(scripts.run_scripts, lambda a: ['filesystem-populated',  a['mounts']['root']], []) )

    seq.append(Task(umountVolumes, A(ans, 'mounts', 'cleanup'), ['cleanup']))
    if ans['target-boot-mode'] == TARGET_BOOT_MODE_LEGACY:
        seq.append(Task(setActiveDiskPartition, A(ans, 'primary-disk', 'boot-partnum', 'primary-partnum', 'partition-table-type'), []))
    seq.append(Task(writeLog, A(ans, 'primary-disk', 'primary-partnum', 'logs-partnum'), []))

    return seq

def prettyLogAnswers(answers):
    for a in answers:
        if a == 'root-password':
            val = (answers[a][0], '< not printed >')
        elif a == 'pool-token':
            val = '< not printed >'
        else:
            val = answers[a]
        logger.log("%s := %s %s" % (a, val, type(val)))

def executeSequence(sequence, seq_name, answers, ui, cleanup):
    answers['cleanup'] = []
    answers['ui'] = ui

    progress_total = reduce(lambda x, y: x + y,
                            [task.progress_scale for task in sequence])

    pd = None
    if ui:
        pd = ui.progress.initProgressDialog(
            "Installing %s" % MY_PRODUCT_BRAND,
            seq_name, progress_total
            )
    logger.log("DISPATCH: NEW PHASE: %s" % seq_name)

    def doCleanup(actions):
        for tag, f, a in actions:
            try:
                apply(f, a)
            except:
                logger.log("FAILED to perform cleanup action %s" % tag)

    def progressCallback(x):
        if ui:
            ui.progress.displayProgressDialog(current + x, pd)

    try:
        current = 0
        for item in sequence:
            if pd:
                if item.progress_text:
                    text = item.progress_text
                else:
                    text = seq_name

                ui.progress.displayProgressDialog(current, pd, updated_text=text)
            updated_state = item.execute(answers, progressCallback)
            if len(updated_state) > 0:
                logger.log(
                    "DISPATCH: Updated state: %s" %
                    str.join("; ", ["%s -> %s" % (v, updated_state[v]) for v in updated_state.keys()])
                    )
                for state_item in updated_state:
                    answers[state_item] = updated_state[state_item]

            current = current + item.progress_scale
    except:
        doCleanup(answers['cleanup'])
        raise
    else:
        if cleanup:
            doCleanup(answers['cleanup'])
            del answers['cleanup']
    finally:
        if ui and pd:
            ui.progress.clearModelessDialog()

def performInstallation(answers, ui_package, interactive):
    logger.log("INPUT ANSWERS DICTIONARY:")
    prettyLogAnswers(answers)
    logger.log("SCRIPTS DICTIONARY:")
    prettyLogAnswers(scripts.script_dict)

    dom0_mem = xcp.dom0.default_memory_for_version(
                    hardware.getHostTotalMemoryKB(),
                    Version.from_string(version.PLATFORM_VERSION)) / 1024
    dom0_vcpus = xcp.dom0.default_vcpus(hardware.getHostTotalCPUs(), dom0_mem)
    default_host_config = { 'dom0-mem': dom0_mem,
                            'dom0-vcpus': dom0_vcpus,
                            'xen-cpuid-masks': [] }
    defaults = { 'branding': {}, 'host-config': {}, 'write-boot-entry': True }

    # update the settings:
    if answers['preserve-settings'] == True:
        defaults.update({ 'guest-disks': [] })

        logger.log("Updating answers dictionary based on existing installation")
        try:
            answers.update(answers['installation-to-overwrite'].readSettings())

            # Use the new default amount of RAM as long as it doesn't result in
            # a decrease from the previous installation. Update the number of
            # dom0 vCPUs since it depends on the amount of RAM assigned.
            if 'dom0-mem' in answers['host-config']:
                answers['host-config']['dom0-mem'] = max(answers['host-config']['dom0-mem'],
                                                         default_host_config['dom0-mem'])
                default_host_config['dom0-vcpus'] = xcp.dom0.default_vcpus(hardware.getHostTotalCPUs(),
                                                                           answers['host-config']['dom0-mem'])
        except Exception as e:
            logger.logException(e)
            raise RuntimeError("Failed to get existing installation settings")

        prettyLogAnswers(answers)
    else:
        defaults.update({ 'master': None,
                          'sr-type': constants.SR_TYPE_LVM,
                          'bootloader-location': constants.BOOT_LOCATION_MBR,
                          'initial-partitions': [],
                          'sr-at-end': True,
                          'sr-on-primary': True,
                          'preserve-first-partition': constants.PRESERVE_IF_UTILITY })

        logger.log("Updating answers dictionary based on defaults")

    for k, v in defaults.items():
        if k not in answers:
            answers[k] = v
    for k, v in default_host_config.items():
        if k not in answers['host-config']:
            answers['host-config'][k] = v
    logger.log("UPDATED ANSWERS DICTIONARY:")
    prettyLogAnswers(answers)

    # Slight hack: we need to write the bridge name to xensource-inventory
    # further down; compute it here based on the admin interface name if we
    # haven't already recorded it as part of reading settings from an upgrade:
    if answers['install-type'] == INSTALL_TYPE_FRESH:
        answers['net-admin-bridge'] = ''
    elif 'net-admin-bridge' not in answers:
        assert answers['net-admin-interface'].startswith("eth")
        answers['net-admin-bridge'] = "xenbr%s" % answers['net-admin-interface'][3:]

    # perform installation:
    prep_seq = getPrepSequence(answers, interactive)
    executeSequence(prep_seq, "Preparing for installation...", answers, ui_package, False)

    # install from main repositories:
    def handleRepos(repos, ans):
        repo_seq = getRepoSequence(ans, repos)
        executeSequence(repo_seq, "Reading package information...", ans, ui_package, False)

    answers['installed-repos'] = {}

    def add_repos(all_repositories, repos):
        """Add repositories to the list, ensuring no duplicates, that the main
        repository is at the beginning, and that the order of the rest is
        maintained."""

        for repo in repos:
            if repo not in all_repositories:
                if repo.identifier() == MAIN_REPOSITORY_NAME:
                    all_repositories.insert(0, repo)
                else:
                    all_repositories.append(repo)

    while True:
        # A list needs to be used rather than a set since the order of updates is
        # important.  However, since the same repository might exist in multiple
        # locations or the same location might be listed multiple times, care is
        # needed to ensure that there are no duplicates.
        all_repositories = []

        # A list of sources coming from the answerfile
        if 'sources' in answers:
            for i in answers['sources']:
                repos = repository.repositoriesFromDefinition(i['media'], i['address'])
                add_repos(all_repositories, repos)

        # A single source coming from an interactive install
        if 'source-media' in answers and 'source-address' in answers:
            repos = repository.repositoriesFromDefinition(answers['source-media'], answers['source-address'])
            add_repos(all_repositories, repos)

        for media, address in answers['extra-repos']:
            repos = repository.repositoriesFromDefinition(media, address)
            add_repos(all_repositories, repos)

        if not all_repositories or all_repositories[0].identifier() != MAIN_REPOSITORY_NAME:
            raise RuntimeError("No main repository found")

        # Check the GPG key of the main repository when a remote repository is used.
        if answers['netinstall-gpg-check']:
            all_repositories[0].setGpgCheck()

        try:
            handleRepos(all_repositories, answers)
            break
        except repository.RepoSecurityConfigError as e:
            if interactive:
                # In net install mode, we cannot have more than 1 remote repository.
                # (The RepoSecurityConfigError exception is only thrown in this mode.)
                #
                # It's difficult to handle many repositories because: How can we
                # retrieve a stable state if all packages of one repository have been
                # installed successfully but there's been installation errors on other
                # repositories?
                assert(answers['source-media'] == 'url')
                assert(len(all_repositories) == 1)
                installer = ui_package.installer
                if installer.screens.reconfigure_repo(str(e)) == REPEAT_STEP:
                    # Reconfigure.
                    answers = installer.reconfigure_source_location_sequence(answers)
                    continue
            raise

    all_repositories[0].installKeys(answers['mounts']['root'])

    # Find repositories that we installed from removable media
    # and eject the media.
    for r in all_repositories:
        if r.accessor().canEject():
            r.accessor().eject()

    if util.isNetInstall():
        for device in getRemovableDeviceList():
            util.runCmd2(['eject', device])

    if interactive:
        # Add supp packs in a loop
        while True:
            media_ans = dict(answers)
            del media_ans['source-media']
            del media_ans['source-address']
            media_ans = ui_package.installer.more_media_sequence(media_ans)
            if 'more-media' not in media_ans or not media_ans['more-media']:
                break

            repos = repository.repositoriesFromDefinition(media_ans['source-media'], media_ans['source-address'])
            repos = set([repo for repo in repos if str(repo) not in answers['installed-repos']])
            if not repos:
                continue
            handleRepos(repos, answers)

            for r in repos:
                if r.accessor().canEject():
                    r.accessor().eject()

    # complete the installation:
    fin_seq = getFinalisationSequence(answers)
    executeSequence(fin_seq, "Completing installation...", answers, ui_package, True)

def configureMCELog(mounts):
    """Disable mcelog on unsupported processors."""

    is_amd = False
    model = 0

    with open('/proc/cpuinfo', 'r') as f:
        for line in f:
            line = line.strip()
            if re.match('vendor_id\s*:\s*AuthenticAMD$', line):
                is_amd = True
                continue
            m = re.match('cpu family\s*:\s*(\d+)$', line)
            if m:
                model = int(m.group(1))

    if is_amd and model >= 16:
        util.runCmd2(['chroot', mounts['root'], 'systemctl', 'disable', 'mcelog'])

def rewriteNTPConf(root, ntp_servers):
    ntpsconf = open("%s/etc/chrony.conf" % root, 'r')
    lines = ntpsconf.readlines()
    ntpsconf.close()

    lines = filter(lambda x: not x.startswith('server '), lines)

    ntpsconf = open("%s/etc/chrony.conf" % root, 'w')
    for line in lines:
        ntpsconf.write(line)
    for server in ntp_servers:
        ntpsconf.write("server %s iburst\n" % server)
    ntpsconf.close()

def setTimeNTP(ntp_servers):
    if len(ntp_servers) > 0:
        rewriteNTPConf('', ntp_servers)

    # This might fail or stall if the network is not set up correctly so set a
    # time limit and don't expect it to succeed.
    if util.runCmd2(['timeout', '15', 'chronyd', '-q']) == 0:
        assert util.runCmd2(['hwclock', '--utc', '--systohc']) == 0

def setTimeManually(localtime, set_time_dialog_dismissed, timezone):
    newtime = localtime + (datetime.datetime.now() - set_time_dialog_dismissed)
    timestr = "%04d-%02d-%02d %02d:%02d:00" % \
              (newtime.year, newtime.month, newtime.day,
               newtime.hour, newtime.minute)

    util.setLocalTime(timestr, timezone=timezone)
    assert util.runCmd2(['hwclock', '--utc', '--systohc']) == 0

def configureNTP(mounts, ntp_servers):
    # If NTP servers were specified, update the NTP config file:
    if len(ntp_servers) > 0:
        rewriteNTPConf(mounts['root'], ntp_servers)

    # now turn on the ntp service:
    util.runCmd2(['chroot', mounts['root'], 'systemctl', 'enable', 'chronyd'])
    util.runCmd2(['chroot', mounts['root'], 'systemctl', 'enable', 'chrony-wait'])

def inspectTargetDisk(disk, existing, initial_partitions, preserve_first_partition, create_sr_part, create_new_partitions):

    uefi_installer = os.path.exists("/sys/firmware/efi")
    logger.log("Installer booted in %s mode" % ("UEFI" if uefi_installer else "legacy"))

    if existing:
        # upgrade, use existing partitioning scheme
        tool = PartitionTool(existing.primary_disk)

        primary_part = tool.partitionNumber(existing.root_device)

        # Determine target install's boot mode and boot partition number
        target_boot_mode = TARGET_BOOT_MODE_LEGACY
        if existing.boot_device:
            boot_partnum = tool.partitionNumber(existing.boot_device)
            boot_part = tool.getPartition(boot_partnum)
            if 'id' in boot_part and boot_part['id'] == GPTPartitionTool.ID_EFI_BOOT:
                target_boot_mode = TARGET_BOOT_MODE_UEFI
        else:
            boot_partnum = primary_part + 3

        if (target_boot_mode == TARGET_BOOT_MODE_UEFI and not uefi_installer) or \
                (target_boot_mode == TARGET_BOOT_MODE_LEGACY and uefi_installer):
            raise RuntimeError("Installer mode (%s) is mismatched with target boot mode (%s)" %
                               ("UEFI" if uefi_installer else "legacy", target_boot_mode))

        logger.log("Upgrading, target_boot_mode: %s" % target_boot_mode)

        # Return install mode and numbers of boot, primary, backup, log, swap and SR partitions
        storage_partition = tool.getPartition(primary_part+2)
        if storage_partition:
            return (target_boot_mode, boot_partnum, primary_part, primary_part+1, primary_part+4, primary_part+5, primary_part+2)
        else:
            return (target_boot_mode, boot_partnum, primary_part, primary_part+1, primary_part+4, primary_part+5, 0)

    tool = PartitionTool(disk)

    # If answerfile says to fake a utility partition then do it here
    if len(initial_partitions) > 0:
        for part in initial_partitions:
            tool.deletePartition(part['number'])
            tool.createPartition(part['id'], part['size'], part['number'])
        tool.commit(log=True)

    # Preserve any utility partitions unless user told us to zap 'em
    primary_part = 1
    if preserve_first_partition == 'true':
        primary_part += 1
    elif preserve_first_partition == constants.PRESERVE_IF_UTILITY:
        utilparts = tool.utilityPartitions()
        primary_part += max(utilparts+[0])
        if primary_part > 2:
            raise RuntimeError("Installer only supports a single Utility Partition at partition 1, but found Utility Partitions at %s" % str(utilparts))

    sr_part = -1
    if create_sr_part:
        sr_part = primary_part+2

    boot_part = max(primary_part + 1, sr_part) + 1

    target_boot_mode = TARGET_BOOT_MODE_UEFI if uefi_installer else TARGET_BOOT_MODE_LEGACY

    logger.log("Fresh install, target_boot_mode: %s" % target_boot_mode)

    # Return install mode and numbers of boot, primary, backup, logs, swap and SR partitions
    if create_new_partitions:
        return (target_boot_mode, boot_part, primary_part, primary_part + 1, primary_part + 4, primary_part + 5, sr_part)
    else:
        return (target_boot_mode, boot_part, primary_part, primary_part + 1, 0, 0, sr_part)

# Determine which partition table type to use
def selectPartitionTableType(disk, install_type, primary_part, create_new_partitions):
    if not constants.GPT_SUPPORT:
        return constants.PARTITION_DOS

    tool = PartitionTool(disk)

    # If not a fresh install then use same partition table as before
    if install_type != INSTALL_TYPE_FRESH:
        return tool.partTableType

    # If we are preserving partition 1 then we need to preserve the
    # partition table type as we are probably chain booting from that.
    if primary_part > 1:
        return tool.partTableType

    # This is a fresh install and we do not need to preserve partition1
    # Use GPT because it is better.
    return constants.PARTITION_GPT

def removeBlockingVGs(disks):
    for vg in diskutil.findProblematicVGs(disks):
        util.runCmd2(['vgreduce', '--removemissing', vg])
        util.runCmd2(['lvremove', vg])
        util.runCmd2(['vgremove', vg])

###
# Functions to write partition tables to disk

def writeDom0DiskPartitions(disk, target_boot_mode, boot_partnum, primary_partnum, backup_partnum, logs_partnum, swap_partnum, storage_partnum, sr_at_end, partition_table_type, create_new_partitions, new_partition_layout):

    # we really don't want to screw this up...
    assert type(disk) == str
    assert disk[:5] == '/dev/'

    if not os.path.exists(disk):
        raise RuntimeError("The disk %s could not be found." % disk)

    # Exit if disk is not big enough even for the pre-Dundee partition layout
    if diskutil.blockSizeToGBSize(diskutil.getDiskDeviceSize(disk)) < constants.min_primary_disk_size_old:
        raise RuntimeError("The disk %s is smaller than %dGB." % (disk, constants.min_primary_disk_size_old))
    # If new partition layout requested: exit if disk is not big enough, otherwise implement it
    elif create_new_partitions:
        if diskutil.blockSizeToGBSize(diskutil.getDiskDeviceSize(disk)) < constants.min_primary_disk_size:
            raise RuntimeError("The disk %s is smaller than %dGB." % (disk, constants.min_primary_disk_size))

    if target_boot_mode == TARGET_BOOT_MODE_UEFI and partition_table_type != constants.PARTITION_GPT:
        raise RuntimeError("UEFI boot requires the partition type to be GPT")

    tool = PartitionTool(disk, partition_table_type)
    for num, part in tool.iteritems():
        if num >= primary_partnum:
            tool.deletePartition(num)

    order = primary_partnum

    if create_new_partitions:

        # Create the new partition layout (5,2,1,4,6,3) or (1,6,3,2,5,7,4)
        # Normal layout                With utility partition
        # 1 - dom0 partition           1 - utility partition
        # 2 - backup partition         2 - dom0 partition
        # 3 - LVM partition            3 - backup partition
        # 4 - Boot partition           4 - LVM partition
        # 5 - logs partition           5 - Boot partition
        # 6 - swap partition           6 - logs partition
        #                              7 - swap partition

        new_partition_layout = True

        # Create logs partition
        # Start the first partition at 1 MiB if there are no other partitions.
        # Otherwise start the partition following the utility partition.
        if order == 1:
            tool.createPartition(tool.ID_LINUX, sizeBytes=logs_size * 2**20, startBytes=2**20, number=logs_partnum, order=order)
        else:
            tool.createPartition(tool.ID_LINUX, sizeBytes=logs_size * 2**20, number=logs_partnum, order=order)
        order += 1

        # Create backup partition
        if backup_partnum > 0:
            tool.createPartition(tool.ID_LINUX, sizeBytes=backup_size * 2**20, number=backup_partnum, order=order)
            order += 1

        # Create dom0 partition
        tool.createPartition(tool.ID_LINUX, sizeBytes=constants.root_size * 2**20, number=primary_partnum, order=order)
        order += 1

        # Create Boot partition
        if partition_table_type == constants.PARTITION_GPT:
            if target_boot_mode == TARGET_BOOT_MODE_UEFI:
                tool.createPartition(tool.ID_EFI_BOOT, sizeBytes=boot_size * 2**20, number=boot_partnum, order=order)
            else:
                tool.createPartition(tool.ID_BIOS_BOOT, sizeBytes=boot_size * 2**20, number=boot_partnum, order=order)
            order += 1

        # Create swap partition
        tool.createPartition(tool.ID_LINUX_SWAP, sizeBytes=swap_size * 2**20, number=swap_partnum, order=order)
        order += 1

        # Create LVM partition
        if storage_partnum > 0:
            tool.createPartition(tool.ID_LINUX_LVM, number=storage_partnum, order=order)
            order += 1

    else:

        # Pre-Dundee partition layout

        # Create Boot partition
        if partition_table_type == constants.PARTITION_GPT:
            if target_boot_mode == TARGET_BOOT_MODE_UEFI:
                tool.createPartition(tool.ID_EFI_BOOT, sizeBytes=boot_size * 2**20, number=boot_partnum, order=order)
            else:
                tool.createPartition(tool.ID_BIOS_BOOT, sizeBytes=boot_size * 2**20, number=boot_partnum, order=order)
            order += 1

        # Create dom0 partition
        root_size = root_gpt_size_old if partition_table_type == constants.PARTITION_GPT else root_mbr_size_old
        tool.createPartition(tool.ID_LINUX, sizeBytes=root_size * 2**20, number=primary_partnum, order=order)
        order += 1

        # Create backup partition
        if backup_partnum > 0:
            tool.createPartition(tool.ID_LINUX, sizeBytes=backup_size_old * 2**20, number=backup_partnum, order=order)
            order += 1

        # Create LVM partition
        if storage_partnum > 0:
            tool.createPartition(tool.ID_LINUX_LVM, number=storage_partnum, order=order)
            order += 1


    if not sr_at_end:
        # For upgrade testing, out-of-order partition layout
        new_parts = {}

        new_parts[primary_partnum] = {'start': tool.partitions[primary_partnum]['start'] + tool.partitions[storage_partnum]['size'],
                                      'size': tool.partitions[primary_partnum]['size'],
                                      'id': tool.partitions[primary_partnum]['id'],
                                      'active': tool.partitions[primary_partnum]['active']}
        if backup_partnum > 0:
            new_parts[backup_partnum] = {'start': new_parts[primary_partnum]['start'] + new_parts[primary_partnum]['size'],
                                         'size': tool.partitions[backup_partnum]['size'],
                                         'id': tool.partitions[backup_partnum]['id'],
                                         'active': tool.partitions[backup_partnum]['active']}

        new_parts[storage_partnum] = {'start': tool.partitions[primary_partnum]['start'],
                                      'size': tool.partitions[storage_partnum]['size'],
                                      'id': tool.partitions[storage_partnum]['id'],
                                      'active': tool.partitions[storage_partnum]['active']}

        for part in (primary_partnum, backup_partnum, storage_partnum):
            if part > 0:
                tool.deletePartition(part)
                tool.createPartition(new_parts[part]['id'], new_parts[part]['size'] * tool.sectorSize, part,
                                     new_parts[part]['start'] * tool.sectorSize, new_parts[part]['active'])

    tool.commit(log=True)

    return new_partition_layout

def writeGuestDiskPartitions(primary_disk, guest_disks, partition_table_type):
    # At the moment this code uses the same partition table type for Guest Disks as it
    # does for the root disk.  But we could choose to always use 'GPT' for guest disks.
    # TODO: Decide!
    for gd in guest_disks:
        if gd != primary_disk:
            # we really don't want to screw this up...
            assert type(gd) == str
            assert gd[:5] == '/dev/'

            tool = PartitionTool(gd, partition_table_type)
            tool.deletePartitions(tool.partitions.keys())
            tool.commit(log=True)


def setActiveDiskPartition(disk, boot_partnum, primary_partnum, partition_table_type):
    tool = PartitionTool(disk, partition_table_type)
    if partition_table_type == PARTITION_GPT:
        tool.commitActivePartitiontoDisk(boot_partnum)
    else:
        tool.commitActivePartitiontoDisk(primary_partnum)

def getSRPhysDevs(primary_disk, storage_partnum, guest_disks):
    def sr_partition(disk):
        if disk == primary_disk:
            return partitionDevice(disk, storage_partnum)
        else:
            return disk

    return [sr_partition(disk) for disk in guest_disks]

def prepareStorageRepositories(mounts, primary_disk, storage_partnum, guest_disks, sr_type):

    if len(guest_disks) == 0 or constants.CC_PREPARATIONS and sr_type != constants.SR_TYPE_EXT:
        logger.log("No storage repository requested.")
        return None

    logger.log("Arranging for storage repositories to be created at first boot...")

    partitions = getSRPhysDevs(primary_disk, storage_partnum, guest_disks)

    sr_type_strings = { constants.SR_TYPE_EXT: 'ext',
                        constants.SR_TYPE_LVM: 'lvm' }
    sr_type_string = sr_type_strings[sr_type]

    # write a config file for the prepare-storage firstboot script:

    links = map(lambda x: diskutil.idFromPartition(x) or x, partitions)
    fd = open(os.path.join(mounts['root'], constants.FIRSTBOOT_DATA_DIR, 'default-storage.conf'), 'w')
    print >>fd, "XSPARTITIONS='%s'" % str.join(" ", links)
    print >>fd, "XSTYPE='%s'" % sr_type_string
    # Legacy names
    print >>fd, "PARTITIONS='%s'" % str.join(" ", links)
    print >>fd, "TYPE='%s'" % sr_type_string
    fd.close()

def make_free_space(mount, required):
    """Make required bytes of free space available on mount by removing files,
    oldest first."""

    def getinfo(dirpath, name):
        path = os.path.join(dirpath, name)
        return os.stat(path).st_mtime, path

    def free_space(path):
        st = os.statvfs(path)
        return st.f_bavail * st.f_frsize

    if free_space(mount) >= required:
        return

    files = []
    dirs = []

    for dirpath, dirnames, filenames in os.walk(mount):
        for i in dirnames:
            dirs.append(getinfo(dirpath, i))
        for i in filenames:
            files.append(getinfo(dirpath, i))

    files.sort()
    dirs.sort()

    for _, path in files:
        os.unlink(path)
        logger.log('Removed %s' % path)
        if free_space(mount) >= required:
            return

    for _, path in dirs:
        shutil.rmtree(path, ignore_errors=True)
        logger.log('Removed %s' % path)
        if free_space(mount) >= required:
            return

    raise RuntimeError("Failed to make enough space available on %s (%d, %d)" % (mount, required, free_space(mount)))

###
# Create dom0 disk file-systems:

def createDom0DiskFilesystems(install_type, disk, target_boot_mode, boot_partnum, primary_partnum, logs_partnum, disk_label_suffix):
    if target_boot_mode == TARGET_BOOT_MODE_UEFI:
        partition = partitionDevice(disk, boot_partnum)
        try:
            util.mkfs(bootfs_type, partition,
                      ["-n", bootfs_label%disk_label_suffix.upper()])
        except Exception as e:
            raise RuntimeError("Failed to create boot filesystem: %s" % e)

    partition = partitionDevice(disk, primary_partnum)
    try:
        util.mkfs(rootfs_type, partition,
                  ["-L", rootfs_label%disk_label_suffix])
    except Exception as e:
        raise RuntimeError("Failed to create root filesystem: %s" % e)

    tool = PartitionTool(disk)
    logs_partition = tool.getPartition(logs_partnum)
    if logs_partition:
        run_mkfs = True

        # If the log partition already exists and is formatted correctly,
        # relabel it. Otherwise create the filesystem.
        partition = partitionDevice(disk, logs_partnum)
        label = None
        try:
            label = diskutil.readExtPartitionLabel(partition)
        except Exception as e:
            # Ignore the exception as it just means the partition needs to be
            # formatted.
            pass
        if install_type != INSTALL_TYPE_FRESH and label and label.startswith(logsfs_label_prefix):
            # If a filesystem which has not been unmounted cleanly is
            # relabelled, it will revert to the original label once it is
            # mounted. To prevent this, fsck the filesystem before relabelling.
            # If any unfixable errors occur or relabelling fails, just recreate
            # the filesystem instead, rather than fail the installation.
            if util.runCmd2(['e2fsck', '-y', partition]) in (0, 1):
                if util.runCmd2(['e2label', partition, constants.logsfs_label % disk_label_suffix]) == 0:
                    run_mkfs = False

        if run_mkfs:
            try:
                util.mkfs(logsfs_type, partition,
                          ["-L", logsfs_label % disk_label_suffix])
            except Exception as e:
                raise RuntimeError("Failed to create logs filesystem: %s" % e)
        else:
            # Ensure enough free space is available
            mount = util.TempMount(partition, 'logs-')
            try:
                make_free_space(mount.mount_point, constants.logs_free_space * 1024 * 1024)
            finally:
                mount.unmount()

def updateBootLoaderLocation(target_boot_mode, partition_table_type, disk, location):
    if target_boot_mode != TARGET_BOOT_MODE_LEGACY:
        return location
    if partition_table_type != PARTITION_DOS:
        return location
    if location != BOOT_LOCATION_MBR:
        return location

    tool = PartitionTool(disk)
    start = min(part[1]['start'] for part in tool.iteritems())
    if start < LBA_PARTITION_MIN:
        logger.log('First partition on disk starts at %d, installing bootloader to partition.' % start)
        return BOOT_LOCATION_PARTITION

    return location

def __mkinitrd(mounts, partition, package, kernel_version, fcoe_interfaces):

    try:
        util.bindMount('/sys', os.path.join(mounts['root'], 'sys'))
        util.bindMount('/dev', os.path.join(mounts['root'], 'dev'))
        util.bindMount('/proc', os.path.join(mounts['root'], 'proc'))
        util.mount('none', os.path.join(mounts['root'], 'tmp'), None, 'tmpfs')

        if isDeviceMapperNode(partition):
            # Generate a valid multipath configuration for the initrd
            action = 'generate-fcoe' if fcoe_interfaces else 'generate-bfs'
            if util.runCmd2(['chroot', mounts['root'],
                             '/etc/init.d/sm-multipath', action]) != 0:
                raise RuntimeError("Failed to generate multipath configuration")

        # Run mkinitrd inside dom0 chroot
        output_file = os.path.join("/boot", "initrd-%s.img" % kernel_version)

        # default to only including host specific kernel modules in initrd
        if os.path.isdir(os.path.join(mounts['root'], 'etc/dracut.conf.d')):
            # disable multipath on root partition
            try:
                if not isDeviceMapperNode(partition):
                    f = open(os.path.join(mounts['root'], 'etc/dracut.conf.d/xs_disable_multipath.conf'), 'w')
                    f.write('omit_dracutmodules+=" multipath "\n')
                    f.close()
            except:
                pass
        else:
            args = ['--theme=/usr/share/splash']

            if isDeviceMapperNode(partition):
                # [multipath-root]: /etc/fstab specifies the rootdev by LABEL so we need this to make sure mkinitrd
                # picks up the master device and not the slave
                args.append('--rootdev='+ partition)
            else:
                args.append('--without-multipath')

            cmd = ['mkinitrd', '--latch']
            cmd.extend( args )
            if util.runCmd2(['chroot', mounts['root']] + cmd) != 0:
                raise RuntimeError("Failed to latch arguments for initrd.")

        cmd = ['new-kernel-pkg', '--install', '--mkinitrd']

        # Save command used to create initrd in <initrd_filename>.cmd
        cmd_logfile = os.path.join(mounts['root'], output_file[1:] + '.cmd')
        cmd_fh = open(cmd_logfile, "w")
        print >>cmd_fh, ' '.join(cmd + ['"$@"', kernel_version])
        cmd_fh.close()

        if util.runCmd2(['chroot', mounts['root'], '/bin/sh', output_file + '.cmd']) != 0:
            raise RuntimeError("Failed to create initrd for %s.  This is often due to using an installer that is not the same version of %s as your installation source." % (kernel_version, MY_PRODUCT_BRAND))

    finally:
        util.umount(os.path.join(mounts['root'], 'sys'))
        util.umount(os.path.join(mounts['root'], 'dev'))
        util.umount(os.path.join(mounts['root'], 'proc'))
        util.umount(os.path.join(mounts['root'], 'tmp'))

def getXenVersion(rootfs_mount):
    """ Return the xen version by interogating the package version in the chroot """
    xen_version = ['rpm', '--root', rootfs_mount, '-q', '--qf', '%{version}', 'xen-hypervisor']
    rc, out = util.runCmd2(xen_version, with_stdout=True)
    if rc != 0:
        return None
    return out

def getKernelVersion(rootfs_mount):
    """ Returns the kernel release (uname -r) of the installed kernel """
    kernel_version = ['rpm', '--root', rootfs_mount, '-q', '--provides', 'kernel']
    rc, out = util.runCmd2(kernel_version, with_stdout=True)
    if rc != 0:
        return None

    try:
        uname_provides = filter(lambda x: x.startswith('kernel-uname-r'), out.split('\n'))
        return uname_provides[0].split('=')[1].strip()
    except:
        pass
    return None

def kernelShortVersion(version):
    """ Return the short kernel version string (i.e., just major.minor). """
    parts = version.split(".")
    return parts[0] + "." + parts[1]

def configureSRMultipathing(mounts, primary_disk):
    # Only called on fresh installs:
    # Configure multipathed SRs iff root disk is multipathed
    fd = open(os.path.join(mounts['root'], constants.FIRSTBOOT_DATA_DIR, 'sr-multipathing.conf'),'w')
    if isDeviceMapperNode(primary_disk):
        fd.write("MULTIPATHING_ENABLED='True'\n")
    else:
        fd.write("MULTIPATHING_ENABLED='False'\n")
    fd.close()

def adjustISCSITimeoutForFile(path):
    iscsiconf = open(path, 'r')
    lines = iscsiconf.readlines()
    iscsiconf.close()

    timeout_key = "node.session.timeo.replacement_timeout"
    wrote_key = False
    iscsiconf = open(path, 'w')
    for line in lines:
        if line.startswith(timeout_key):
            iscsiconf.write("%s = %d\n" % (timeout_key, MPATH_ISCSI_TIMEOUT))
            wrote_key = True
        else:
            iscsiconf.write(line)
    if not wrote_key:
        iscsiconf.write("%s = %d\n" % (timeout_key, MPATH_ISCSI_TIMEOUT))

    iscsiconf.close()

def configureISCSI(mounts, primary_disk):
    if not diskutil.is_iscsi(primary_disk):
        return

    iname = diskutil.get_initiator_name()

    with open(os.path.join(mounts['root'], 'etc/iscsi/initiatorname.iscsi'), 'w') as f:
        f.write('InitiatorName=%s\n' % (iname,))

    # Create IQN file for XAPI
    with open(os.path.join(mounts['root'], 'etc/firstboot.d/data/iqn.conf'), 'w') as f:
        f.write("IQN='%s'" % iname)

    if util.runCmd2(['chroot', mounts['root'],
                     'systemctl', 'enable', 'iscsid']):
        raise RuntimeError("Failed to enable iscsid")
    if util.runCmd2(['chroot', mounts['root'],
                     'systemctl', 'enable', 'iscsi']):
        raise RuntimeError("Failed to enable iscsi")

    diskutil.write_iscsi_records(mounts, primary_disk)

    # Reduce the timeout when using multipath
    if isDeviceMapperNode(primary_disk):
        adjustISCSITimeoutForFile("%s/etc/iscsi/iscsid.conf" % mounts['root'])

def mkinitrd(mounts, primary_disk, primary_partnum, fcoe_interfaces):
    xen_version = getXenVersion(mounts['root'])
    if xen_version is None:
        raise RuntimeError("Unable to determine Xen version.")
    xen_kernel_version = getKernelVersion(mounts['root'])
    if not xen_kernel_version:
        raise RuntimeError("Unable to determine kernel version.")
    partition = partitionDevice(primary_disk, primary_partnum)


    __mkinitrd(mounts, partition, 'kernel-xen', xen_kernel_version, fcoe_interfaces)

def prepFallback(mounts, primary_disk, primary_partnum):
    kernel_version =  getKernelVersion(mounts['root'])

    # Copy /boot/xen-xxxx.gz to /boot/xen-fallback.gz
    xen_gz = os.path.realpath(mounts['root'] + "/boot/xen.gz")
    src = os.path.join(mounts['root'], "boot", os.path.basename(xen_gz))
    dst = os.path.join(mounts['root'], 'boot/xen-fallback.gz')
    shutil.copyfile(src, dst)

    # Copy /boot/vmlinuz-yyyy to /boot/vmlinuz-fallback
    src = os.path.join(mounts['root'], 'boot/vmlinuz-%s' % kernel_version)
    dst = os.path.join(mounts['root'], 'boot/vmlinuz-fallback')
    shutil.copyfile(src, dst)

    # Extra modules to include in the fallback initrd.  Include all
    # currently loaded modules so the network module is picked up.
    modules = []
    proc_modules = open('/proc/modules', 'r')
    for line in proc_modules:
        modules.append(line.split(' ')[0])
    proc_modules.close()

    # Generate /boot/initrd-fallback.img.
    cmd = ['mkinitrd', '--verbose']
    for mod in modules:
        cmd.append('--with=%s' % mod)
    cmd += ['/boot/initrd-fallback.img', kernel_version]
    if util.runCmd2(['chroot', mounts['root']] + cmd):
        raise RuntimeError("Failed to generate fallback initrd")

def buildBootLoaderMenu(mounts, xen_version, xen_kernel_version, boot_config, serial, boot_serial, host_config, primary_disk, disk_label_suffix, fcoe_interfaces):
    short_version = kernelShortVersion(xen_kernel_version)
    common_xen_params = "dom0_mem=%dM,max:%dM" % ((host_config['dom0-mem'],) * 2)
    common_xen_unsafe_params = "watchdog ucode=scan dom0_max_vcpus=1-%d" % host_config['dom0-vcpus']
    safe_xen_params = ("nosmp noreboot noirqbalance no-mce no-bootscrub "
                       "no-numa no-hap no-mmcfg iommu=off max_cstate=0 "
                       "nmi=ignore allow_unsafe")
    xen_mem_params = "crashkernel=256M,below=4G"

    # CA-103933 - AMD PCI-X Hypertransport Tunnel IOAPIC errata
    rc, out = util.runCmd2(['lspci', '-n'], with_stdout=True)
    if rc == 0 and ('1022:7451' in out or '1022:7459' in out):
        common_xen_params += " ioapic_ack=old"

    common_kernel_params = "root=LABEL=%s ro nolvm hpet=disable" % constants.rootfs_label%disk_label_suffix
    kernel_console_params = "console=hvc0"

    if diskutil.is_iscsi(primary_disk):
        common_kernel_params += " rd.iscsi.ibft=1 rd.iscsi.firmware=1"

    if diskutil.is_raid(primary_disk):
        common_kernel_params += " rd.auto"

    if fcoe_interfaces:
        for interface in fcoe_interfaces:
            common_kernel_params += " fcoe=%s:%s" % (netutil.getHWAddr(interface), 'nodcb' if fcoeutil.hw_lldp_capable(interface) else 'dcb')

    e = bootloader.MenuEntry(hypervisor="/boot/xen.gz",
                             hypervisor_args=' '.join([common_xen_params, common_xen_unsafe_params, xen_mem_params, "console=vga vga=mode-0x0311"]),
                             kernel="/boot/vmlinuz-%s-xen" % short_version,
                             kernel_args=' '.join([common_kernel_params, kernel_console_params, "console=tty0 quiet vga=785 splash plymouth.ignore-serial-consoles"]),
                             initrd="/boot/initrd-%s-xen.img" % short_version, title=MY_PRODUCT_BRAND,
                             root=constants.rootfs_label%disk_label_suffix)
    boot_config.append("xe", e)
    boot_config.default = "xe"
    if serial:
        xen_serial_params = "%s console=%s,vga" % (serial.xenFmt(), serial.port)

        e = bootloader.MenuEntry(hypervisor="/boot/xen.gz",
                                 hypervisor_args=' '.join([xen_serial_params, common_xen_params, common_xen_unsafe_params, xen_mem_params]),
                                 kernel="/boot/vmlinuz-%s-xen" % short_version,
                                 kernel_args=' '.join([common_kernel_params, "console=tty0", kernel_console_params]),
                                 initrd="/boot/initrd-%s-xen.img" % short_version, title=MY_PRODUCT_BRAND+" (Serial)",
                                 root=constants.rootfs_label%disk_label_suffix)
        boot_config.append("xe-serial", e)
        if boot_serial:
            boot_config.default = "xe-serial"
        e = bootloader.MenuEntry(hypervisor="/boot/xen.gz",
                                 hypervisor_args=' '.join([safe_xen_params, common_xen_params, xen_serial_params]),
                                 kernel="/boot/vmlinuz-%s-xen" % short_version,
                                 kernel_args=' '.join(["earlyprintk=xen", common_kernel_params, "console=tty0", kernel_console_params]),
                                 initrd="/boot/initrd-%s-xen.img" % short_version, title=MY_PRODUCT_BRAND+" in Safe Mode",
                                 root=constants.rootfs_label%disk_label_suffix)
        boot_config.append("safe", e)

    e = bootloader.MenuEntry(hypervisor="/boot/xen-fallback.gz",
                             hypervisor_args=' '.join([common_xen_params, common_xen_unsafe_params, xen_mem_params]),
                             kernel="/boot/vmlinuz-fallback",
                             kernel_args=' '.join([common_kernel_params, kernel_console_params, "console=tty0"]),
                             initrd="/boot/initrd-fallback.img",
                             title="%s (Xen %s / Linux %s)" % (MY_PRODUCT_BRAND, xen_version, xen_kernel_version),
                             root=constants.rootfs_label%disk_label_suffix)
    boot_config.append("fallback", e)
    if serial:
        e = bootloader.MenuEntry(hypervisor="/boot/xen-fallback.gz",
                                 hypervisor_args=' '.join([xen_serial_params, common_xen_params, common_xen_unsafe_params, xen_mem_params]),
                                 kernel="/boot/vmlinuz-fallback",
                                 kernel_args=' '.join([common_kernel_params, "console=tty0", kernel_console_params]),
                                 initrd="/boot/initrd-fallback.img",
                                 title="%s (Serial, Xen %s / Linux %s)" % (MY_PRODUCT_BRAND, xen_version, xen_kernel_version),
                                 root=constants.rootfs_label%disk_label_suffix)
        boot_config.append("fallback-serial", e)

def installBootLoader(mounts, disk, partition_table_type, boot_partnum, primary_partnum, target_boot_mode, branding,
                      disk_label_suffix, location, write_boot_entry, install_type, serial=None,
                      boot_serial=None, host_config=None, fcoe_interface=None):
    assert(location in [constants.BOOT_LOCATION_MBR, constants.BOOT_LOCATION_PARTITION])

    # prepare extra mounts for installing bootloader:
    util.bindMount("/dev", "%s/dev" % mounts['root'])
    util.bindMount("/sys", "%s/sys" % mounts['root'])
    util.bindMount("/proc", "%s/proc" % mounts['root'])

    try:
        if host_config:
            s = serial and {'port': serial.id, 'baud': int(serial.baud)} or None

            if target_boot_mode == TARGET_BOOT_MODE_UEFI:
                fn = os.path.join(mounts['boot'], "efi/EFI/xenserver/grub.cfg")
            else:
                fn = os.path.join(mounts['boot'], "grub/grub.cfg")
            boot_config = bootloader.Bootloader('grub2', fn,
                                                timeout=constants.BOOT_MENU_TIMEOUT,
                                                serial=s, location=location)
            xen_version = getXenVersion(mounts['root'])
            if xen_version is None:
                raise RuntimeError("Unable to determine Xen version.")
            xen_kernel_version = getKernelVersion(mounts['root'])
            if not xen_kernel_version:
                raise RuntimeError("Unable to determine kernel version.")
            buildBootLoaderMenu(mounts, xen_version, xen_kernel_version, boot_config,
                                serial, boot_serial, host_config, disk,
                                disk_label_suffix, fcoe_interface)
            util.assertDir(os.path.dirname(fn))
            boot_config.commit()

        root_partition = partitionDevice(disk, primary_partnum)
        if target_boot_mode == TARGET_BOOT_MODE_UEFI:
            if write_boot_entry:
                setEfiBootEntry(mounts, disk, boot_partnum, install_type, branding)
        else:
            if location == constants.BOOT_LOCATION_MBR:
                if diskutil.is_raid(disk):
                    for member in diskutil.getDeviceSlaves(disk):
                        installGrub2(mounts, member, False)
                else:
                    installGrub2(mounts, disk, False)
            else:
                installGrub2(mounts, root_partition, True)

        if serial:
            # ensure a getty will run on the serial console
            old = open("%s/etc/inittab" % mounts['root'], 'r')
            new = open('/tmp/inittab', 'w')

            for line in old:
                if line.startswith("s%d:" % serial.id):
                    new.write(re.sub(r'getty \S+ \S+', "getty %s %s" % (serial.dev, serial.baud), line))
                else:
                    new.write(line)

            old.close()
            new.close()
            shutil.move('/tmp/inittab', "%s/etc/inittab" % mounts['root'])
    finally:
        # done installing - undo our extra mounts:
        util.umount("%s/proc" % mounts['root'])
        util.umount("%s/sys" % mounts['root'])
        util.umount("%s/dev" % mounts['root'])

def setEfiBootEntry(mounts, disk, boot_partnum, install_type, branding):
    def check_efibootmgr_err(rc, err, install_type, err_type):
        if rc != 0:
            if install_type == INSTALL_TYPE_REINSTALL:
                logger.error("%s: %s" % (err_type, err))
            else:
                raise RuntimeError("%s: %s" % (err_type, err))

    # First remove existing entries
    rc, out, err = util.runCmd2(["chroot", mounts['root'], "/usr/sbin/efibootmgr"], True, True)
    check_efibootmgr_err(rc, err, install_type, "Failed to run efibootmgr")
    for line in out.splitlines():
        match = re.match("Boot([0-9a-fA-F]{4})\\*? +(?:XenServer|%s)$" % branding['product-brand'], line)
        if match:
            bootnum = match.group(1)
            rc, err = util.runCmd2(["chroot", mounts['root'], "/usr/sbin/efibootmgr",
                                    "--delete-bootnum", "--bootnum", bootnum], with_stderr=True)
            check_efibootmgr_err(rc, err, install_type, "Failed to remove efi boot entry")

    # Then add a new one
    rc, err = util.runCmd2(["chroot", mounts['root'], "/usr/sbin/efibootmgr", "-c",
                            "-L", branding['product-brand'], "-l", '\\' + "EFI/xenserver/grubx64.efi".replace('/', '\\'),
                            "-d", disk, "-p", str(boot_partnum)], with_stderr=True)
    check_efibootmgr_err(rc, err, install_type, "Failed to run efibootmgr")

def installGrub2(mounts, disk, force):
    if force:
        rc, err = util.runCmd2(["chroot", mounts['root'], "/usr/sbin/grub-install", "--target=i386-pc", "--force", disk], with_stderr=True)
    else:
        rc, err = util.runCmd2(["chroot", mounts['root'], "/usr/sbin/grub-install", "--target=i386-pc", disk], with_stderr=True)
    if rc != 0:
        raise RuntimeError("Failed to install bootloader: %s" % err)

def installExtLinux(mounts, disk, partition_table_type, location=constants.BOOT_LOCATION_MBR):

    # As of v4.02 syslinux installs comboot modules under /boot/extlinux/.
    # However we continue to copy the ones we need to /boot so we can write the config file there.
    # We need to do this because old installers are needed to restore old XS images from the backup
    # partition, and these need to read the config on the current partition.  Oops.
    # This also means we avoid find and fix all the other scripts which assume extlinux.conf is under /boot.

    rc, err = util.runCmd2(["chroot", mounts['root'], "/sbin/extlinux", "--install", "/boot"], with_stderr=True)
    if rc != 0:
        raise RuntimeError("Failed to install bootloader: %s" % err)

    for m in ["mboot", "menu", "chain"]:
        if not os.path.exists("%s/%s.c32" % (mounts['boot'], m)):
            os.link("%s/extlinux/%s.c32" % (mounts['boot'], m), "%s/%s.c32" % (mounts['boot'], m))

    # must be able to restore pre-6.0 systems
    base_dir = mounts['root'] + "/usr/share/syslinux"
    if not os.path.exists(base_dir):
        base_dir = mounts['root']+"/usr/lib/syslinux"
    if location == constants.BOOT_LOCATION_MBR:
        if partition_table_type == constants.PARTITION_DOS:
            mbr = base_dir + "/mbr.bin"
        elif partition_table_type == constants.PARTITION_GPT:
            mbr = base_dir + "/gptmbr.bin"
        else:
            raise Exception("Only DOS and GPT partition tables supported")

        # Write image to MBR
        logger.log("Installing %s to %s" % (mbr, disk))
        assert os.path.exists(mbr)
        assert util.runCmd2(["dd", "if=%s" % mbr, "of=%s" % disk]) == 0

##########
# mounting and unmounting of various volumes

def mountVolumes(primary_disk, boot_partnum, primary_partnum, logs_partnum, cleanup, target_boot_mode):
    mounts = {'root': '/tmp/root',
              'boot': '/tmp/root/boot'}

    rootp = partitionDevice(primary_disk, primary_partnum)
    util.assertDir('/tmp/root')
    util.mount(rootp, mounts['root'])
    rc, out = util.runCmd2(['cat', '/proc/mounts'], with_stdout=True)
    logger.log(out)
    tool = PartitionTool(primary_disk)
    logs_partition = tool.getPartition(logs_partnum)

    util.assertDir(constants.EXTRA_SCRIPTS_DIR)
    util.mount('tmpfs', constants.EXTRA_SCRIPTS_DIR, ['size=2m'], 'tmpfs')
    util.assertDir(os.path.join(mounts['root'], 'mnt'))
    util.bindMount(constants.EXTRA_SCRIPTS_DIR, os.path.join(mounts['root'], 'mnt'))
    new_cleanup = cleanup + [ ("umount-/tmp/root", util.umount, (mounts['root'], )),
                              ("umount-/tmp/root/mnt",  util.umount, (os.path.join(mounts['root'], 'mnt'), )) ]

    if target_boot_mode == TARGET_BOOT_MODE_UEFI:
        mounts['esp'] = '/tmp/root/boot/efi'
        bootp = partitionDevice(primary_disk, boot_partnum)
        util.assertDir(os.path.join(mounts['root'], 'boot', 'efi'))
        util.mount(bootp, mounts['esp'])
        new_cleanup.append(("umount-/tmp/root/boot/efi", util.umount, (mounts['esp'], )))
    if logs_partition:
        mounts['logs'] = os.path.join(mounts['root'], 'var/log')
        util.assertDir(mounts['logs'])
        util.mount(partitionDevice(primary_disk, logs_partnum), mounts['logs'])
        new_cleanup.append(("umount-/tmp/root/var/log", util.umount, (mounts['logs'], )))
    return mounts, new_cleanup

def umountVolumes(mounts, cleanup, force=False):
    def filterCleanup(tag, _, __):
        return (not tag.startswith("umount-%s" % mounts['root']) and
                not tag.startswith("umount-%s" % os.path.join(mounts['root'], 'mnt')) and
                not tag.startswith("umount-%s" % mounts['boot']))

    util.umount(os.path.join(mounts['root'], 'mnt'))
    util.umount(constants.EXTRA_SCRIPTS_DIR)
    if 'esp' in mounts:
        util.umount(mounts['esp'])
    if 'logs' in mounts:
        util.umount(mounts['logs'])
    util.umount(mounts['root'])
    cleanup = filter(filterCleanup, cleanup)
    return cleanup

##########
# second stage install helpers:

def writeKeyboardConfiguration(mounts, keymap):
    util.assertDir("%s/etc/sysconfig/" % mounts['root'])
    if not keymap:
        keymap = 'us'
        logger.log("No keymap specified, defaulting to 'us'")

    vconsole = open("%s/etc/vconsole.conf" % mounts['root'], 'w')
    vconsole.write("KEYMAP=%s\n" % keymap)
    vconsole.close()

def prepareSwapfile(mounts, primary_disk, swap_partnum, disk_label_suffix):

    tool = PartitionTool(primary_disk)

    swap_partition = tool.getPartition(swap_partnum)

    if swap_partition:
        util.bindMount("/proc", "%s/proc" % mounts['root'])
        util.bindMount("/sys", "%s/sys" % mounts['root'])
        util.bindMount("/dev", "%s/dev" % mounts['root'])
        dev = partitionDevice(primary_disk, swap_partnum)
        while True:
            # The uuid of a swap partition overlaps the same position as the
            # superblock magic for a MINIX filesystem (offset 0x410 or 0x418).
            # The uuid might by coincidence match the superblock magic. The
            # magic is only two bytes long and there are several different
            # magic identifiers which increases the chances of matching.  If
            # this happens, blkid marks the partition as ambivalent because it
            # contains multiple signatures which prevents by-label symlinks
            # from being created and the swap partition from being activated.
            # Avoid this by running mkswap until the filesystem is no longer
            # ambivalent.
            util.runCmd2(['chroot', mounts['root'], 'mkswap', '-L', constants.swap_label%disk_label_suffix, dev])
            rc, out = util.runCmd2(['chroot', mounts['root'], 'blkid', '-o', 'udev', '-p', dev], with_stdout=True)
            keys = [line.strip().split('=')[0] for line in out.strip().split('\n')]
            if 'ID_FS_AMBIVALENT' not in keys:
                break
        util.umount("%s/dev" % mounts['root'])
        util.umount("%s/proc" % mounts['root'])
        util.umount("%s/sys" % mounts['root'])
    else:
        util.assertDir("%s/var/swap" % mounts['root'])
        util.runCmd2(['dd', 'if=/dev/zero',
                      'of=%s' % os.path.join(mounts['root'], constants.swap_file.lstrip('/')),
                      'bs=1024', 'count=%d' % (constants.swap_file_size * 1024)])
        util.bindMount("/proc", "%s/proc" % mounts['root'])
        util.bindMount("/sys", "%s/sys" % mounts['root'])
        util.runCmd2(['chroot', mounts['root'], 'mkswap', constants.swap_file])
        util.umount("%s/proc" % mounts['root'])
        util.umount("%s/sys" % mounts['root'])

def writeFstab(mounts, target_boot_mode, primary_disk, logs_partnum, swap_partnum, disk_label_suffix):

    tool = PartitionTool(primary_disk)
    swap_partition = tool.getPartition(swap_partnum)
    logs_partition = tool.getPartition(logs_partnum)

    fstab = open(os.path.join(mounts['root'], 'etc/fstab'), "w")
    fstab.write("LABEL=%s    /         %s     defaults   1  1\n" % (rootfs_label%disk_label_suffix, rootfs_type))
    if target_boot_mode == TARGET_BOOT_MODE_UEFI:
        fstab.write("LABEL=%s    /boot/efi         %s     defaults   0  2\n" % (bootfs_label%disk_label_suffix.upper(), bootfs_type))

    if swap_partition:
        fstab.write("LABEL=%s          swap      swap   defaults   0  0\n" % constants.swap_label%disk_label_suffix)
    else:
        if os.path.exists(os.path.join(mounts['root'], constants.swap_file.lstrip('/'))):
            fstab.write("%s          swap      swap   defaults   0  0\n" % (constants.swap_file))
    if logs_partition:
        fstab.write("LABEL=%s    /var/log         %s     defaults   0  2\n" % (logsfs_label%disk_label_suffix, logsfs_type))

    # This should be removed when the packaging CARs are done
    if logs_partition:
        # partition therefore daily rotate
        logrotate = open(os.path.join(mounts['root'], 'etc/sysconfig/logrotate'), "w")
        logrotate.write("BUDGET_MB=4000")
        logrotate.close()

def enableAgent(mounts, network_backend, services):
    if network_backend == constants.NETWORK_BACKEND_VSWITCH:
        util.runCmd2(['chroot', mounts['root'],
                      'systemctl', 'enable',
                                   'openvswitch.service',
                                   'openvswitch-xapi-sync.service'])

    util.assertDir(os.path.join(mounts['root'], constants.BLOB_DIRECTORY))

    # Enable/disable miscellaneous services
    actMap = {'enabled': 'enable', 'disabled': 'disable'}
    for (service, state) in services.iteritems():
        action = 'disable' if constants.CC_PREPARATIONS and state is None else actMap.get(state)
        if action:
            util.runCmd2(['chroot', mounts['root'], 'systemctl', action, service + '.service'])

def writeResolvConf(mounts, hn_conf, ns_conf):
    (manual_hostname, hostname) = hn_conf
    (manual_nameservers, nameservers) = ns_conf

    if manual_hostname:
        # 'search' option in resolv.conf
        try:
            dot = hostname.index('.')
            if dot + 1 != len(hostname):
                resolvconf = open("%s/etc/resolv.conf" % mounts['root'], 'w')
                dname = hostname[dot + 1:]
                resolvconf.write("search %s\n" % dname)
                resolvconf.close()
        except:
            pass
    else:
        hostname = 'localhost.localdomain'

    # /etc/hostname:
    eh = open('%s/etc/hostname' % mounts['root'], 'w')
    eh.write(hostname + "\n")
    eh.close()


    if manual_nameservers:

        resolvconf = open("%s/etc/resolv.conf" % mounts['root'], 'a')
        for ns in nameservers:
            if ns != "":
                resolvconf.write("nameserver %s\n" % ns)
        resolvconf.close()

def writeMachineID(mounts):
    util.bindMount("/dev", "%s/dev" % mounts['root'])

    try:
        # Remove any existing machine-id file
        try:
            os.unlink(os.path.join(mounts['root'], 'etc/machine-id'))
        except:
            pass
        util.runCmd2(['chroot', mounts['root'], 'systemd-machine-id-setup'])
    finally:
        util.umount("%s/dev" % mounts['root'])

def setTimeZone(mounts, tz):
    # make the localtime link:
    assert util.runCmd2(['ln', '-sf', '../usr/share/zoneinfo/%s' % tz,
                         '%s/etc/localtime' % mounts['root']]) == 0

def setRootPassword(mounts, root_pwd):
    # avoid using shell here to get around potential security issues.  Also
    # note that chpasswd needs -m to allow longer passwords to work correctly
    # but due to a bug in the RHEL5 version of this tool it segfaults when this
    # option is specified, so we have to use passwd instead if we need to
    # encrypt the password.  Ugh.
    (pwdtype, root_password) = root_pwd
    if pwdtype == 'pwdhash':
        cmd = ["/usr/sbin/chroot", mounts["root"], "chpasswd", "-e"]
        pipe = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     close_fds=True)
        pipe.communicate('root:%s\n' % root_password)
        assert pipe.wait() == 0
    else:
        cmd = ["/usr/sbin/chroot", mounts['root'], "passwd", "--stdin", "root"]
        pipe = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     close_fds=True)
        pipe.communicate(root_password + "\n")
        assert pipe.wait() == 0

# write /etc/sysconfig/network-scripts/* files
def configureNetworking(mounts, admin_iface, admin_bridge, admin_config, hn_conf, ns_conf, nethw, preserve_settings, network_backend):
    """ Writes configuration files that the firstboot scripts will consume to
    configure interfaces via the CLI.  Writes a loopback device configuration.
    to /etc/sysconfig/network-scripts, and removes any other configuration
    files from that directory."""

    (manual_hostname, hostname) = hn_conf
    (manual_nameservers, nameservers) = ns_conf
    domain = None
    if manual_hostname:
        dot = hostname.find('.')
        if dot != -1:
            domain = hostname[dot+1:]

    # always set network backend
    util.assertDir(os.path.join(mounts['root'], 'etc/xensource'))
    nwconf = open("%s/etc/xensource/network.conf" % mounts["root"], "w")
    nwconf.write("%s\n" % network_backend)
    logger.log("Writing %s to /etc/xensource/network.conf" % network_backend)
    nwconf.close()

    mgmt_conf_file = os.path.join(mounts['root'], constants.FIRSTBOOT_DATA_DIR, 'management.conf')
    if not os.path.exists(mgmt_conf_file):
        mc = open(mgmt_conf_file, 'w')
        print >>mc, "LABEL='%s'" % admin_iface
        print >>mc, "MODE='%s'" % netinterface.NetInterface.getModeStr(admin_config.mode)
        if admin_config.mode == netinterface.NetInterface.Static:
            print >>mc, "IP='%s'" % admin_config.ipaddr
            print >>mc, "NETMASK='%s'" % admin_config.netmask
            if admin_config.gateway:
                print >>mc, "GATEWAY='%s'" % admin_config.gateway
            if manual_nameservers:
                print >>mc, "DNS='%s'" % (','.join(nameservers),)
            if domain:
                print >>mc, "DOMAIN='%s'" % domain
        print >>mc, "MODEV6='%s'" % netinterface.NetInterface.getModeStr(admin_config.modev6)
        if admin_config.modev6 == netinterface.NetInterface.Static:
            print >>mc, "IPv6='%s'" % admin_config.ipv6addr
            if admin_config.ipv6_gateway:
                print >>mc, "IPv6_GATEWAY='%s'" % admin_config.ipv6_gateway
        if admin_config.vlan:
            print >>mc, "VLAN='%d'" % admin_config.vlan
        mc.close()

    if preserve_settings:
        return

    # Clean install only below this point

    util.assertDir(os.path.join(mounts['root'], constants.FIRSTBOOT_DATA_DIR))

    network_scripts_dir = os.path.join(mounts['root'], 'etc/sysconfig/network-scripts')

    # remove any files that may be present in the filesystem already,
    # particularly those created by kudzu:
    network_scripts = os.listdir(network_scripts_dir)
    for s in filter(lambda x: x.startswith('ifcfg-'), network_scripts):
        os.unlink(os.path.join(network_scripts_dir, s))

    # write the configuration file for the loopback interface
    lo = open(os.path.join(network_scripts_dir, 'ifcfg-lo'), 'w')
    lo.write("DEVICE=lo\n")
    lo.write("IPADDR=127.0.0.1\n")
    lo.write("NETMASK=255.0.0.0\n")
    lo.write("NETWORK=127.0.0.0\n")
    lo.write("BROADCAST=127.255.255.255\n")
    lo.write("ONBOOT=yes\n")
    lo.write("NAME=loopback\n")
    lo.close()

    save_dir = os.path.join(mounts['root'], constants.FIRSTBOOT_DATA_DIR, 'initial-ifcfg')
    util.assertDir(save_dir)

    # now we need to write /etc/sysconfig/network
    nfd = open("%s/etc/sysconfig/network" % mounts["root"], "w")
    nfd.write("NETWORKING=yes\n")
    if admin_config.modev6:
        nfd.write("NETWORKING_IPV6=yes\n")
        util.runCmd2(['chroot', mounts['root'], 'systemctl', 'enable', 'ip6tables'])
    else:
        nfd.write("NETWORKING_IPV6=no\n")
        netutil.disable_ipv6_module(mounts["root"])
    nfd.write("IPV6_AUTOCONF=no\n")
    nfd.write('NTPSERVERARGS="iburst prefer"\n')
    nfd.close()

    if network_backend == constants.NETWORK_BACKEND_VSWITCH:
        # CA-51684: blacklist bridge module
        bfd = open("%s/etc/modprobe.d/blacklist-bridge.conf" % mounts["root"], "w")
        bfd.write("install bridge /bin/true\n")
        bfd.close()

    # EA-1069 - write static-rules.conf and dynamic-rules.conf
    if not os.path.exists(os.path.join(mounts['root'], 'etc/sysconfig/network-scripts/interface-rename-data/.from_install/')):
        os.makedirs(os.path.join(mounts['root'], 'etc/sysconfig/network-scripts/interface-rename-data/.from_install/'), 0775)

    netutil.static_rules.path = os.path.join(mounts['root'], 'etc/sysconfig/network-scripts/interface-rename-data/static-rules.conf')
    netutil.static_rules.save()
    netutil.static_rules.path = os.path.join(mounts['root'], 'etc/sysconfig/network-scripts/interface-rename-data/.from_install/static-rules.conf')
    netutil.static_rules.save()

    netutil.dynamic_rules.path = os.path.join(mounts['root'], 'etc/sysconfig/network-scripts/interface-rename-data/dynamic-rules.json')
    netutil.dynamic_rules.save()
    netutil.dynamic_rules.path = os.path.join(mounts['root'], 'etc/sysconfig/network-scripts/interface-rename-data/.from_install/dynamic-rules.json')
    netutil.dynamic_rules.save()

def writeXencommons(controlID, mounts):
    with open(os.path.join(mounts['root'], constants.XENCOMMONS_FILE), "r") as f:
        contents = f.read()

    dom0_uuid_str = ("XEN_DOM0_UUID=%s" % controlID)
    contents = re.sub('.*XEN_DOM0_UUID=.*', dom0_uuid_str, contents)

    with open(os.path.join(mounts['root'], constants.XENCOMMONS_FILE), "w") as f:
        f.write(contents)

def writeInventory(installID, controlID, mounts, primary_disk, backup_partnum, storage_partnum, guest_disks, admin_bridge, branding, admin_config, host_config, new_partition_layout, partition_table_type, install_type):
    inv = open(os.path.join(mounts['root'], constants.INVENTORY_FILE), "w")
    if 'product-brand' in branding:
       inv.write("PRODUCT_BRAND='%s'\n" % branding['product-brand'])
    if PRODUCT_NAME:
       inv.write("PRODUCT_NAME='%s'\n" % PRODUCT_NAME)
    if 'product-version' in branding:
       inv.write("PRODUCT_VERSION='%s'\n" % branding['product-version'])
    if PRODUCT_VERSION_TEXT:
       inv.write("PRODUCT_VERSION_TEXT='%s'\n" % PRODUCT_VERSION_TEXT)
    if PRODUCT_VERSION_TEXT_SHORT:
       inv.write("PRODUCT_VERSION_TEXT_SHORT='%s'\n" % PRODUCT_VERSION_TEXT_SHORT)
    if COMPANY_NAME:
       inv.write("COMPANY_NAME='%s'\n" % COMPANY_NAME)
    if COMPANY_NAME_SHORT:
       inv.write("COMPANY_NAME_SHORT='%s'\n" % COMPANY_NAME_SHORT)
    if COMPANY_PRODUCT_BRAND:
       inv.write("COMPANY_PRODUCT_BRAND='%s'\n" % COMPANY_PRODUCT_BRAND)
    if BRAND_CONSOLE:
       inv.write("BRAND_CONSOLE='%s'\n" % BRAND_CONSOLE)
    if BRAND_CONSOLE_URL:
       inv.write("BRAND_CONSOLE_URL='%s'\n" % BRAND_CONSOLE_URL)
    inv.write("PLATFORM_NAME='%s'\n" % branding['platform-name'])
    inv.write("PLATFORM_VERSION='%s'\n" % branding['platform-version'])

    layout = 'ROOT,BACKUP'
    if partition_table_type == constants.PARTITION_GPT:
        if new_partition_layout:
            layout += ',LOG,BOOT,SWAP'
        else:
            layout += ',BOOT'
    if storage_partnum > 0:
        layout += ',SR'
    inv.write("PARTITION_LAYOUT='%s'\n" % layout)

    if 'product-build' in branding:
        inv.write("BUILD_NUMBER='%s'\n" % branding['product-build'])
    inv.write("INSTALLATION_DATE='%s'\n" % str(datetime.datetime.now()))
    inv.write("PRIMARY_DISK='%s'\n" % (diskutil.idFromPartition(primary_disk) or primary_disk))
    if backup_partnum > 0:
        inv.write("BACKUP_PARTITION='%s'\n" % (diskutil.idFromPartition(partitionDevice(primary_disk, backup_partnum)) or partitionDevice(primary_disk, backup_partnum)))
    inv.write("INSTALLATION_UUID='%s'\n" % installID)
    inv.write("CONTROL_DOMAIN_UUID='%s'\n" % controlID)
    inv.write("DOM0_MEM='%d'\n" % host_config['dom0-mem'])
    inv.write("DOM0_VCPUS='%d'\n" % host_config['dom0-vcpus'])
    inv.write("MANAGEMENT_INTERFACE='%s'\n" % admin_bridge)
    # Default to IPv4 unless we have only got an IPv6 admin interface
    if ((not admin_config.mode) and admin_config.modev6):
        inv.write("MANAGEMENT_ADDRESS_TYPE='IPv6'\n")
    else:
        inv.write("MANAGEMENT_ADDRESS_TYPE='IPv4'\n")
    if constants.CC_PREPARATIONS and install_type == constants.INSTALL_TYPE_FRESH:
        inv.write("CC_PREPARATIONS='true'\n")
    inv.close()

def touchSshAuthorizedKeys(mounts):
    util.assertDir("%s/root/.ssh/" % mounts['root'])
    fh = open("%s/root/.ssh/authorized_keys" % mounts['root'], 'a')
    fh.close()

def importYumAndRpmGpgKeys(mounts):
    # Python script that uses yum functions to import the GPG key for our repositories
    import_yum_keys = """#!/bin/env python
from __future__ import print_function
from yum import YumBase

def retTrue(*args, **kwargs):
    return True

base = YumBase()
for repo in base.repos.repos.itervalues():
    if repo.id.startswith('xcp-ng'):
        print("*** Importing GPG key for repository %s - %s" % (repo.id, repo.name))
        base.getKeyForRepo(repo, callback=retTrue)
"""
    internal_tmp_filepath = '/tmp/import_yum_keys.py'
    external_tmp_filepath = mounts['root'] + internal_tmp_filepath
    with open(external_tmp_filepath, 'w') as f:
        f.write(import_yum_keys)
    # bind mount /dev, necessary for NSS initialization without which RPM won't work
    util.bindMount('/dev', "%s/dev" % mounts['root'])
    try:
        util.runCmd2(['chroot', mounts['root'], 'python', internal_tmp_filepath])
        util.runCmd2(['chroot', mounts['root'], 'rpm', '--import', '/etc/pki/rpm-gpg/RPM-GPG-KEY-xcpng'])
    finally:
        util.umount("%s/dev" % mounts['root'])
        os.unlink(external_tmp_filepath)

def postInstallAltKernel(mounts, kernel_alt):
    """ Install our alternate kernel. Must be called after the bootloader installation. """
    if not kernel_alt:
        logger.log('kernel-alt not installed')
        return

    util.bindMount("/proc", "%s/proc" % mounts['root'])
    util.bindMount("/sys", "%s/sys" % mounts['root'])
    util.bindMount("/dev", "%s/dev" % mounts['root'])

    try:
        rc, out = util.runCmd2(['chroot', mounts['root'], 'rpm', '-q', 'kernel-alt', '--qf', '%{version}'],
                               with_stdout=True)
        version = out
        # Generate the initrd as it was disabled during initial installation
        util.runCmd2(['chroot', mounts['root'], 'dracut', '-f', '/boot/initrd-%s.img' % version, version])

        # Update grub
        util.runCmd2(['chroot', mounts['root'], 'python', '/usr/lib/python2.7/site-packages/xcp/updategrub.py', '--add', version])
    finally:
        util.umount("%s/dev" % mounts['root'])
        util.umount("%s/sys" % mounts['root'])
        util.umount("%s/proc" % mounts['root'])

################################################################################
# OTHER HELPERS

# This function is not supposed to throw exceptions so that it can be used
# within the main exception handler.
def writeLog(primary_disk, primary_partnum, logs_partnum):
    tool = PartitionTool(primary_disk)

    logs_partition = tool.getPartition(logs_partnum)

    if logs_partition:
        try:
            bootnode = partitionDevice(primary_disk, logs_partnum)
            primary_fs = util.TempMount(bootnode, 'install-')
            try:
                log_location = os.path.join(primary_fs.mount_point, "installer")
                if os.path.islink(log_location):
                    log_location = os.path.join(primary_fs.mount_point, os.readlink(log_location).lstrip("/"))
                util.assertDir(log_location)
                xelogging.collectLogs(log_location, os.path.join(primary_fs.mount_point,"root"))
            except:
                pass
            primary_fs.unmount()
        except:
            pass
    else:
        try:
            bootnode = partitionDevice(primary_disk, primary_partnum)
            primary_fs = util.TempMount(bootnode, 'install-')
            try:
                log_location = os.path.join(primary_fs.mount_point, "var/log/installer")
                if os.path.islink(log_location):
                    log_location = os.path.join(primary_fs.mount_point, os.readlink(log_location).lstrip("/"))
                util.assertDir(log_location)
                xelogging.collectLogs(log_location, os.path.join(primary_fs.mount_point,"root"))
            except:
                pass
            primary_fs.unmount()
        except:
            pass

def writei18n(mounts):
    path = os.path.join(mounts['root'], 'etc/locale.conf')
    fd = open(path, 'w')
    fd.write('LANG="en_US.UTF-8"\n')
    fd.close()

def verifyRepos(sources, ui):
    """ Check repos are accessible """

    for i in sources:
        repo_good = False

        if ui:
            if tui.repo.check_repo_def((i['media'], i['address']), False) == tui.repo.REPOCHK_NO_ERRORS:
               repo_good = True
        else:
            try:
                repos = repository.repositoriesFromDefinition(i['media'], i['address'])
                if len(repos) > 0:
                    repo_good = True
            except:
                pass

        if not repo_good:
            raise RuntimeError("Unable to access repository (%s, %s)" % (i['media'], i['address']))

def getUpgrader(source):
    """ Returns an appropriate upgrader for a given source. """
    return upgrade.getUpgrader(source)

def prepareTarget(progress_callback, upgrader, *args):
    return upgrader.prepareTarget(progress_callback, *args)

def doBackup(progress_callback, upgrader, *args):
    return upgrader.doBackup(progress_callback, *args)

def prepareUpgrade(progress_callback, upgrader, *args):
    """ Gets required state from existing installation. """
    return upgrader.prepareUpgrade(progress_callback, *args)

def completeUpgrade(upgrader, *args):
    """ Puts back state into new filesystem. """
    return upgrader.completeUpgrade(*args)
