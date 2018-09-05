# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Functions to perform restore from backup partition, including UI.
#
# written by Andrew Peace

import backend
import product
from disktools import *
import xelogging
import diskutil
import util
import os
import os.path
import constants
import re
import tempfile
import shutil
import xcp.bootloader as bootloader

def restoreFromBackup(backup, progress = lambda x: ()):
    """ Restore files from backup_partition to the root partition on disk.
    Call progress with a value between 0 and 100.  Re-install bootloader.  Fails if 
    backup is not same version as the CD in use."""

    disk = backup.root_disk
    tool = PartitionTool(disk)
    _, boot_partnum, primary_partnum, backup_partnum, logs_partnum, swap_partnum, _ = backend.inspectTargetDisk(disk, None, [], constants.PRESERVE_IF_UTILITY, True, True)
    backup_version = backup.version
    limit_version = product.THIS_PLATFORM_VERSION
    logs_partition = tool.getPartition(logs_partnum)
    boot_partition = tool.getPartition(boot_partnum)
    root_partition = partitionDevice(disk, primary_partnum)

    backup_fs = util.TempMount(backup.partition, 'backup-', options = ['ro'])
    inventory = util.readKeyValueFile(os.path.join(backup_fs.mount_point, constants.INVENTORY_FILE), strip_quotes = True)
    backup_partition_layout = []
    if 'PARTITION_LAYOUT' in inventory:  # Present from XS 7.0
        backup_partition_layout = inventory['PARTITION_LAYOUT'].split(',')
    backup_fs.unmount()

    (boot, _, _, _, logs) = diskutil.probeDisk(disk)

    xelogging.log("BACKUP DISK PARTITION LAYOUT: %s" % backup_partition_layout)

    if not logs[0] and boot[0] and not backup_partition_layout: # From 7.x (no new layout - yes Boot partition) to 6.x
        restoreWithoutRepartButUEFI(backup, progress)
    else:
        doRestore(backup, progress, backup_partition_layout, logs[0])

def doRestore(backup, progress, backup_partition_layout, has_logs_partition):

    backup_partition = backup.partition
    backup_version = backup.version
    disk = backup.root_disk
    tool = PartitionTool(disk)
    _, boot_partnum, primary_partnum, backup_partnum, logs_partnum, swap_partnum, _ = backend.inspectTargetDisk(disk, None, [], constants.PRESERVE_IF_UTILITY, True, True)
    limit_version = product.THIS_PLATFORM_VERSION
    logs_partition = tool.getPartition(logs_partnum)
    boot_partition = tool.getPartition(boot_partnum)

    assert backup_partition.startswith('/dev/')
    assert disk.startswith('/dev/')

    label = None
    bootlabel = None
    if has_logs_partition and not backup_partition_layout: # From 7.x (new layout) to 6.x
        restore_partition = partitionDevice(disk, logs_partnum)
    else:
        restore_partition = partitionDevice(disk, primary_partnum)
    xelogging.log("Restoring to partition %s." % restore_partition)

    tool = PartitionTool(disk)
    boot_part = tool.getPartition(boot_partnum)
    boot_device = partitionDevice(disk, boot_partnum) if boot_part else None
    efi_boot = boot_part and boot_part['id'] == GPTPartitionTool.ID_EFI_BOOT

    # determine current location of bootloader
    current_location = 'unknown'
    try:
        root_fs = util.TempMount(restore_partition, 'root-', options = ['ro'], boot_device = boot_device)
        try:
            boot_config = bootloader.Bootloader.loadExisting(root_fs.mount_point)
            current_location = boot_config.location
            xelogging.log("Bootloader currently in %s" % current_location)
        finally:
            root_fs.unmount()
    except:
        pass

    # mount the backup fs
    backup_fs = util.TempMount(backup_partition, 'restore-backup-', options = ['ro'])
    try:
        # extract the bootloader config
        boot_config = bootloader.Bootloader.loadExisting(backup_fs.mount_point)
        if boot_config.src_fmt == 'grub':
            raise RuntimeError, "Backup uses grub bootloader which is no longer supported - " + \
                "to restore please use a version of the installer that matches the backup partition"

        # format the restore partition(s):
        if util.runCmd2(['mkfs.%s' % constants.rootfs_type, restore_partition]) != 0:
            raise RuntimeError, "Failed to create root filesystem"
        if efi_boot:
            if util.runCmd2(['mkfs.vfat', boot_device]) != 0:
                raise RuntimeError, "Failed to create boot filesystem"

        # mount restore partition:
        dest_fs = util.TempMount(restore_partition, 'restore-dest-')
        try:

            # copy files from the backup partition to the restore partition:
            objs = filter(lambda x: x not in ['lost+found', '.xen-backup-partition', '.xen-gpt.bin'],
                          os.listdir(backup_fs.mount_point))
            for i in range(len(objs)):
                obj = objs[i]
                xelogging.log("Restoring subtree %s..." % obj)
                progress((i * 100) / len(objs))

                # Use 'cp' here because Python's copying tools are useless and
                # get stuck in an infinite loop when copying e.g. /dev/null.
                if util.runCmd2(['cp', '-a', os.path.join(backup_fs.mount_point, obj),
                                 dest_fs.mount_point]) != 0:
                    raise RuntimeError, "Failed to restore %s directory" % obj

            xelogging.log("Data restoration complete.  About to re-install bootloader.")

            location = boot_config.location
            m = re.search(r'root=LABEL=(\S+)', boot_config.menu[boot_config.default].kernel_args)
            if m:
                label = m.group(1)
            if location == constants.BOOT_LOCATION_PARTITION and current_location == constants.BOOT_LOCATION_MBR:
                # if bootloader in the MBR it's probably not safe to restore with it
                # on the partition
                xelogging.log("Bootloader is currently installed to MBR, restoring to MBR instead of partition")
                location = constants.BOOT_LOCATION_MBR

            with open(os.path.join(backup_fs.mount_point, 'etc', 'fstab'), 'r') as fstab:
                for line in fstab:
                    m = re.match(r'LABEL=(\S+)\s+/boot/efi\s', line)
                    if m:
                        bootlabel = m.group(1)

            mounts = {'root': dest_fs.mount_point, 'boot': os.path.join(dest_fs.mount_point, 'boot')}

            # prepare extra mounts for installing bootloader:
            util.bindMount("/dev", "%s/dev" % dest_fs.mount_point)
            util.bindMount("/sys", "%s/sys" % dest_fs.mount_point)
            util.bindMount("/proc", "%s/proc" % dest_fs.mount_point)
            if boot_config.src_fmt == 'grub2':
                if efi_boot:
                    branding = util.readKeyValueFile(os.path.join(backup_fs.mount_point, constants.INVENTORY_FILE))
                    branding['product-brand'] = branding['PRODUCT_BRAND']
                    backend.setEfiBootEntry(mounts, disk, boot_partnum, branding)
                else:
                    if location == constants.BOOT_LOCATION_MBR:
                        backend.installGrub2(mounts, disk, False)
                    else:
                        backend.installGrub2(mounts, restore_partition, True)
            else:
                backend.installExtLinux(mounts, disk, probePartitioningScheme(disk), location)

            # restore bootloader configuration
            dst_file = boot_config.src_file.replace(backup_fs.mount_point, dest_fs.mount_point, 1)
            util.assertDir(os.path.dirname(dst_file))
            boot_config.commit(dst_file)
        finally:
            util.umount("%s/proc" % dest_fs.mount_point)
            util.umount("%s/sys" % dest_fs.mount_point)
            util.umount("%s/dev" % dest_fs.mount_point)
            dest_fs.unmount()
    finally:
        backup_fs.unmount()

    if not label:
        raise RuntimeError, "Failed to find label required for root filesystem."
    if efi_boot and not bootlabel:
        raise RuntimeError("Failed to find label required for boot filesystem.")

    if util.runCmd2(['e2label', restore_partition, label]) != 0:
        raise RuntimeError, "Failed to label root partition"

    if bootlabel:
        if util.runCmd2(['fatlabel', boot_device, bootlabel]) != 0:
            raise RuntimeError, "Failed to label boot partition"

    if has_logs_partition:
        if not backup_partition_layout: # From 7.x (new layout) to 6.x
            # Delete backup, dom0, Boot and swap partitions
            tool.deletePartition(backup_partnum)
            tool.deletePartition(primary_partnum)
            tool.deletePartition(boot_partnum)
            tool.deletePartition(swap_partnum)

            # Rename logs partition to be n.1
            tool.renamePartition(srcNumber = logs_partnum, destNumber = primary_partnum, overwrite = False)

            # Create 4GB backup partition
            tool.createPartition(tool.ID_LINUX, sizeBytes = constants.backup_size_old * 2**20, startBytes = tool.partitionEnd(primary_partnum) + tool.sectorSize, number = backup_partnum)

            # Commit partition table and mark dom0 disk as bootable
            tool.commit(log = True)
            tool.commitActivePartitiontoDisk(primary_partnum)

            xelogging.log("Bootloader restoration complete.")
            xelogging.log("Restore successful.")
            backend.writeLog(disk, primary_partnum, logs_partnum)
        elif 'LOG' in backup_partition_layout: # From 7.x (new layout) to 7.x (new layout)
            tool.commitActivePartitiontoDisk(boot_partnum)
            rdm_label = label.split("-")[1]
            logs_part = partitionDevice(disk, logs_partnum)
            swap_part = partitionDevice(disk, swap_partnum)
            if util.runCmd2(['e2label', logs_part, constants.logsfs_label%rdm_label]) != 0:
                raise RuntimeError, "Failed to label logs partition"
            if util.runCmd2(['swaplabel', '-L', constants.swap_label%rdm_label, swap_part]) != 0:
                raise RuntimeError, "Failed to label swap partition"

def restoreWithoutRepartButUEFI(backup, progress):

    backup_partition = backup.partition
    disk = backup.root_disk

    assert backup_partition.startswith('/dev/')
    assert disk.startswith('/dev/')

    # Restore the partition layout
    backup_fs = util.TempMount(backup_partition, 'restore-backup-', options = ['ro'])
    gpt_bin = None
    try:
        src_bin = os.path.join(backup_fs.mount_point, '.xen-gpt.bin')
        if os.path.exists(src_bin):
            gpt_bin = tempfile.mktemp()
            shutil.copyfile(src_bin, gpt_bin)
    finally:
        backup_fs.unmount()

    if gpt_bin:
        xelogging.log("Restoring partition layout")
        rc, err = util.runCmd2(["sgdisk", "-l", gpt_bin, disk], with_stderr = True)
        if rc != 0:
            raise RuntimeError, "Failed to restore partition layout: %s" % err

    label = None
    bootlabel = None
    _, boot_partnum, primary_partnum, backup_partnum, logs_partnum, swap_partnum, _ = backend.inspectTargetDisk(disk, None, [], constants.PRESERVE_IF_UTILITY, True, True)
    restore_partition = partitionDevice(disk, primary_partnum)
    xelogging.log("Restoring to partition %s." % restore_partition)

    tool = PartitionTool(disk)
    boot_part = tool.getPartition(boot_partnum)
    boot_device = partitionDevice(disk, boot_partnum) if boot_part else None
    efi_boot = boot_part and boot_part['id'] == GPTPartitionTool.ID_EFI_BOOT

    # determine current location of bootloader
    current_location = 'unknown'
    try:
        root_fs = util.TempMount(restore_partition, 'root-', options = ['ro'], boot_device = boot_device)
        try:
            boot_config = bootloader.Bootloader.loadExisting(root_fs.mount_point)
            current_location = boot_config.location
            xelogging.log("Bootloader currently in %s" % current_location)
        finally:
            root_fs.unmount()
    except:
        pass

    # mount the backup fs
    backup_fs = util.TempMount(backup_partition, 'restore-backup-', options = ['ro'])
    try:        
        # extract the bootloader config
        boot_config = bootloader.Bootloader.loadExisting(backup_fs.mount_point)
        if boot_config.src_fmt == 'grub':
            raise RuntimeError, "Backup uses grub bootloader which is no longer supported - " + \
                "to restore please use a version of the installer that matches the backup partition"

        # format the restore partition(s):
        if util.runCmd2(['mkfs.%s' % constants.rootfs_type, restore_partition]) != 0:
            raise RuntimeError, "Failed to create root filesystem"
        if efi_boot:
            if util.runCmd2(['mkfs.vfat', boot_device]) != 0:
                raise RuntimeError, "Failed to create boot filesystem"

        # mount restore partition:
        dest_fs = util.TempMount(restore_partition, 'restore-dest-', boot_device = boot_device, boot_mount_point = '/boot/efi')
        try:

            # copy files from the backup partition to the restore partition:
            objs = filter(lambda x: x not in ['lost+found', '.xen-backup-partition', '.xen-gpt.bin'],
                          os.listdir(backup_fs.mount_point))
            for i in range(len(objs)):
                obj = objs[i]
                xelogging.log("Restoring subtree %s..." % obj)
                progress((i * 100) / len(objs))

                # Use 'cp' here because Python's copying tools are useless and
                # get stuck in an infinite loop when copying e.g. /dev/null.
                if util.runCmd2(['cp', '-a', os.path.join(backup_fs.mount_point, obj),
                                 dest_fs.mount_point]) != 0:
                    raise RuntimeError, "Failed to restore %s directory" % obj

            xelogging.log("Data restoration complete.  About to re-install bootloader.")

            location = boot_config.location
            m = re.search(r'root=LABEL=(\S+)', boot_config.menu[boot_config.default].kernel_args)
            if m:
                label = m.group(1)
            if location == constants.BOOT_LOCATION_PARTITION and current_location == constants.BOOT_LOCATION_MBR:
                # if bootloader in the MBR it's probably not safe to restore with it
                # on the partition
                xelogging.log("Bootloader is currently installed to MBR, restoring to MBR instead of partition")
                location = constants.BOOT_LOCATION_MBR

            with open(os.path.join(backup_fs.mount_point, 'etc', 'fstab'), 'r') as fstab:
                for line in fstab:
                    m = re.match(r'LABEL=(\S+)\s+/boot/efi\s', line)
                    if m:
                        bootlabel = m.group(1)

            mounts = {'root': dest_fs.mount_point, 'boot': os.path.join(dest_fs.mount_point, 'boot')}

            # prepare extra mounts for installing bootloader:
            util.bindMount("/dev", "%s/dev" % dest_fs.mount_point)
            util.bindMount("/sys", "%s/sys" % dest_fs.mount_point)
            util.bindMount("/proc", "%s/proc" % dest_fs.mount_point)
            if boot_config.src_fmt == 'grub2':
                if efi_boot:
                    branding = util.readKeyValueFile(os.path.join(backup_fs.mount_point, constants.INVENTORY_FILE))
                    branding['product-brand'] = branding['PRODUCT_BRAND']
                    backend.setEfiBootEntry(mounts, disk, boot_partnum, branding)
                else:
                    if location == constants.BOOT_LOCATION_MBR:
                        backend.installGrub2(mounts, disk, False)
                    else:
                        backend.installGrub2(mounts, restore_partition, True)
            else:
                backend.installExtLinux(mounts, disk, probePartitioningScheme(disk), location)

            # restore bootloader configuration
            dst_file = boot_config.src_file.replace(backup_fs.mount_point, dest_fs.mount_point, 1)
            util.assertDir(os.path.dirname(dst_file))
            boot_config.commit(dst_file)
        finally:
            util.umount("%s/proc" % dest_fs.mount_point)
            util.umount("%s/sys" % dest_fs.mount_point)
            util.umount("%s/dev" % dest_fs.mount_point)
            dest_fs.unmount()
    finally:
        backup_fs.unmount()

    if not label:
        raise RuntimeError, "Failed to find label required for root filesystem."
    if efi_boot and not bootlabel:
        raise RuntimeError("Failed to find label required for boot filesystem.")

    if util.runCmd2(['e2label', restore_partition, label]) != 0:
        raise RuntimeError, "Failed to label root partition"

    if bootlabel:
        if util.runCmd2(['fatlabel', boot_device, bootlabel]) != 0:
            raise RuntimeError, "Failed to label boot partition"

    xelogging.log("Bootloader restoration complete.")
    xelogging.log("Restore successful.")
    backend.writeLog(disk, primary_partnum, logs_partnum)
