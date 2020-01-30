# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Disk discovery and utilities
#
# written by Andrew Peace

import re, sys
import os.path
import errno
import constants
import CDROM
import fcntl
import glob
import util
import netutil
from util import dev_null
import xcp.logger as logger
from disktools import *
import time
from snackutil import ButtonChoiceWindowEx

use_mpath = False

def mpath_cli_is_working():
    regex = re.compile("switchgroup")
    try:
        (rc,stdout) = util.runCmd2(["multipathd","-k"], with_stdout=True, inputtext="help")
        m=regex.search(stdout)
        return bool(m)
    except:
        return False

def wait_for_multipathd():
    for i in range(0,120):
        if mpath_cli_is_working():
            return
        time.sleep(1)
    msg = "Unable to contact Multipathd daemon"
    logger.log(msg)
    raise Exception(msg)

def mpath_part_scan(force=False):
    global use_mpath

    if not force and not use_mpath:
        return 0
    ret = createMpathPartnodes()
    if ret == 0:
         util.runCmd2(util.udevsettleCmd())
    return ret

def mpath_enable():
    global use_mpath
    assert 0 == util.runCmd2(['modprobe','dm-multipath'])

    if not os.path.exists('/etc/multipath.conf') and os.path.exists('/etc/multipath.conf.disabled'):
        os.rename('/etc/multipath.conf.disabled', '/etc/multipath.conf')

    # launch manually to make possible to wait initialization
    util.runCmd2(["/sbin/multipath", "-v0", "-B"])
    time.sleep(1)
    util.runCmd2(util.udevsettleCmd())

    # This creates maps for all disks at start of day (because -e is ommitted)
    assert 0 == util.runCmd2('multipathd -d > /var/log/multipathd 2>&1 &')
    wait_for_multipathd()
    # CA-48440: Cope with lost udev events
    util.runCmd2(["multipathd","-k"], inputtext="reconfigure")

    # Tell DM to create partition nodes for newly created mpath devices
    assert 0 == mpath_part_scan(True)
    logger.log("created multipath device(s)");
    use_mpath = True

def mpath_disable():
    destroyMpathPartnodes()
    util.runCmd2(['killall','multipathd'])
    util.runCmd2(['/sbin/multipath','-F'])
    use_mpath = False

# hd* -> (ide has majors 3, 22, 33, 34, 56, 57, 88, 89, 90, 91, each major has
# two disks, with minors 0... and 64...)
ide_majors = [ 3, 22, 33, 34, 56, 57, 88, 89, 90, 91 ]
disk_nodes  = [ (x, 0) for x in ide_majors ]
disk_nodes += [ (x, 64) for x in ide_majors ]

# sd* -> (sd-mod has majors 8, 65 ... 71, 128 ... 135: each device has eight minors, each
# major has sixteen disks).
# Extended minors are used for disk 257 and above and the major number wraps back to 8
# thus disk 257 is major 8 minor 256, disk 258 is
disk_nodes += [ (8, x * 16) for x in range(16) ]
disk_nodes += [ (65, x * 16) for x in range(16) ]
disk_nodes += [ (66, x * 16) for x in range(16) ]
disk_nodes += [ (67, x * 16) for x in range(16) ]
disk_nodes += [ (68, x * 16) for x in range(16) ]
disk_nodes += [ (69, x * 16) for x in range(16) ]
disk_nodes += [ (70, x * 16) for x in range(16) ]
disk_nodes += [ (71, x * 16) for x in range(16) ]
disk_nodes += [ (128, x * 16) for x in range(16) ]
disk_nodes += [ (129, x * 16) for x in range(16) ]
disk_nodes += [ (130, x * 16) for x in range(16) ]
disk_nodes += [ (131, x * 16) for x in range(16) ]
disk_nodes += [ (132, x * 16) for x in range(16) ]
disk_nodes += [ (133, x * 16) for x in range(16) ]
disk_nodes += [ (134, x * 16) for x in range(16) ]
disk_nodes += [ (135, x * 16) for x in range(16) ]

# xvd* -> (blkfront has major 202: each device has 15 minors)
disk_nodes += [ (202, x * 16) for x in range(16) ]

# /dev/cciss : c[0-7]d[0-15]: Compaq Next Generation Drive Array
# /dev/ida   : c[0-7]d[0-15]: Compaq Intelligent Drive Array
for major in range(72, 80) + range(104, 112):
    disk_nodes += [ (major, x * 16) for x in range(16) ]

# /dev/rd    : c[0-7]d[0-31]: Mylex DAC960 PCI RAID controller
for major in range(48, 56):
    disk_nodes += [ (major, x * 8) for x in range(32) ]

# /dev/md    : md has major 9: each device has 15 minors)
disk_nodes += [ (9, x * 16) for x in range(16) ]

# /dev/mmcblk: mmcblk has major 179, each device usually (per kernel) has 7 minors
disk_nodes += [ (179, x * 8) for x in range(32) ]

def getDiskList():
    # read the partition tables:
    parts = open("/proc/partitions")
    partlines = map(lambda x: re.sub(" +", " ", x).strip(),
                    parts.readlines())
    parts.close()

    # parse it:
    disks = []
    for l in partlines:
        try:
            (major, minor, size, name) = l.split(" ")
            (major, minor, size) = (int(major), int(minor) % 256, int(size))
            if hasDeviceMapperHolder("/dev/" + name.replace("!","/")):
                # skip device that cannot be used
                continue
            if isDeviceMapperNode("/dev/" + name.replace("!","/")):
                # dm-* devices get added later as mapper/* devices
                continue
            if (major, minor) in disk_nodes:
                if major == 202 and isRemovable("/dev/" + name): # Ignore PV CDROM devices
                    continue
                disks.append(name.replace("!", "/"))
            # Handle LOCAL/EXPERIMENTAL and Block Extended Major devices
            if 240 <= major <= 254 or major == 259:
                rc, out = util.runCmd2(['/bin/lsblk', '-d', '-n', '-o', 'TYPE', "/dev/" + name.replace("!","/")],
                                       with_stdout=True)
                if rc == 0 and out.strip() not in ['part', 'md']:
                    disks.append(name.replace("!", "/"))

        except:
            # it wasn't an actual entry, maybe the headers or something:
            continue
    # Add multipath nodes to list
    disks.extend(map(lambda node: node.replace('/dev/',''), getMpathNodes()))
    # Add md RAID nodes to list
    disks.extend(map(lambda node: node.replace('/dev/',''), getMdNodes()))

    return disks

def create_raid(configuration):
    if configuration:
        for raid_device, members in configuration.viewitems():
            # allows for idempotence
            if not os.path.exists(raid_device):
                for dev in members:
                    util.runCmd2(['mdadm', '--zero-superblock', '--force', dev])
                    # let it fail without catching
                cmd = ['mdadm', '--create', raid_device, '--run', '--metadata=1.0', '--level=mirror',
                       '--raid-devices=%s' % (len(members))] + members
                rc, out, err = util.runCmd2(cmd, with_stdout=True, with_stderr=True)
                if rc != 0:
                    raise Exception('Error running: %s\n%s\n\n%s' % (' '.join(cmd), out, err))

def getPartitionList():
    disks = getDiskList()
    rv  = []
    for disk in disks:
        rv.extend(partitionsOnDisk(disk))
    return rv

def partitionsOnDisk(disk):
    if disk.startswith('/dev/'):
        disk = disk[5:]
    if isDeviceMapperNode('/dev/' + disk):
        name = disk.split('/',1)[1]
        partitions = filter(lambda s: re.match(name + r'p?\d+$', s), os.listdir('/dev/mapper/'))
        partitions = map(lambda s: "mapper/%s" % s, partitions)
    else:
        name = disk.replace("/", "!")
        partitions = filter(lambda s: s.startswith(name), os.listdir('/sys/block/%s' % name))
        partitions = map(lambda n: n.replace("!","/"), partitions)

    return partitions

def getQualifiedDiskList():
    return map(lambda x: getQualifiedDeviceName(x), getDiskList())

def getQualifiedPartitionList():
    return [getQualifiedDeviceName(x) for x in getPartitionList()]

def getRemovableDeviceList():
    devs = os.listdir('/sys/block')
    removable_devs = []
    for d in devs:
        if isRemovable(d):
            removable_devs.append(d.replace("!", "/"))

    return removable_devs

def removable(device):
    if device.startswith('/dev/'):
        device = device[5:]

    # CA-25624 - udev maps sr* to scd*
    if device.startswith('scd'):
        device = 'sr'+device[3:]

    return device in getRemovableDeviceList()

def getQualifiedDeviceName(disk):
    return "/dev/%s" % disk

# Given a partition (e.g. /dev/sda1), get the id symlink:
def idFromPartition(partition):
    symlink = None
    v, out = util.runCmd2(util.udevinfoCmd() + ['-q', 'symlink', '-n', partition], with_stdout=True)
    prefixes = ['disk/by-id/edd', 'disk/by-id/dm-name-', 'disk/by-id/dm-uuid-', 'disk/by-id/lvm-pv-uuid-', 'disk/by-id/cciss-']
    if v == 0:
        for link in out.split():
            if link.startswith('disk/by-id') and not True in map(lambda x : link.startswith(x), prefixes):
                symlink = '/dev/'+link
                break
    return symlink

# Given a id symlink (e.g. /dev/disk/by-id/scsi-...), get the device
def partitionFromId(symlink):
    return os.path.realpath(symlink)

def __readOneLineFile__(filename):
    try:
        f = open(filename)
        value = f.readline()
        f.close()
        return value
    except Exception as e:
        raise e

def getDiskDeviceVendor(dev):

    # For Multipath nodes return info about 1st slave
    if not dev.startswith("/dev/"):
        dev = '/dev/' + dev
    if isDeviceMapperNode(dev):
        return getDiskDeviceVendor(getDeviceSlaves(dev)[0])
    if is_raid(dev):
        vendors = set(map(getDiskDeviceVendor, getDeviceSlaves(dev)))
        return '/'.join(vendors)

    if dev.startswith("/dev/"):
        dev = re.match("/dev/(.*)", dev).group(1)
    dev = dev.replace("/", "!")
    if os.path.exists("/sys/block/%s/device/vendor" % dev):
        return __readOneLineFile__("/sys/block/%s/device/vendor" % dev).strip(' \n')
    else:
        return ""

def getDiskDeviceModel(dev):

    # For Multipath nodes return info about 1st slave
    if not dev.startswith("/dev/"):
        dev = '/dev/' + dev
    if isDeviceMapperNode(dev):
        return getDiskDeviceModel(getDeviceSlaves(dev)[0])
    if is_raid(dev):
        models = set(map(getDiskDeviceModel, getDeviceSlaves(dev)))
        return '/'.join(models)

    if dev.startswith("/dev/"):
        dev = re.match("/dev/(.*)", dev).group(1)
    dev = dev.replace("/", "!")
    if os.path.exists("/sys/block/%s/device/model" % dev):
        return __readOneLineFile__("/sys/block/%s/device/model" % dev).strip('  \n')
    else:
        return ""

def getDiskDeviceSize(dev):

    # For Multipath nodes return info about 1st slave
    if not dev.startswith("/dev/"):
        dev = '/dev/' + dev
    if isDeviceMapperNode(dev):
        return getDiskDeviceSize(getDeviceSlaves(dev)[0])

    if dev.startswith("/dev/"):
        dev = re.match("/dev/(.*)", dev).group(1)
    dev = dev.replace("/", "!")
    if os.path.exists("/sys/block/%s/device/block/size" % dev):
        return int(__readOneLineFile__("/sys/block/%s/device/block/size" % dev))
    elif os.path.exists("/sys/block/%s/size" % dev):
        return int(__readOneLineFile__("/sys/block/%s/size" % dev))

def getDiskSerialNumber(dev):
    # For Multipath nodes return info about 1st slave
    if not dev.startswith("/dev/"):
        dev = '/dev/' + dev
    if isDeviceMapperNode(dev):
        return getDiskSerialNumber(getDeviceSlaves(dev)[0])
    if is_raid(dev):
        serials = set(map(getDiskSerialNumber, getDeviceSlaves(dev)))
        return '/'.join(serials)

    rc, out = util.runCmd2(['/bin/sdparm', '-q', '-i', '-p', 'sn', dev], with_stdout=True)
    if rc == 0:
        lines = out.split('\n')
        if len(lines) >= 2:
            return lines[1].strip()
    return ""

def isRemovable(path):

    if path.startswith('/dev/mapper') or path.startswith('/dev/dm-') or path.startswith('dm-'):
        return False

    if path.startswith("/dev/"):
        dev = re.match("/dev/(.*)", path).group(1)
    else:
        dev = path

    dev = dev.replace("/", "!")

    if dev.startswith("xvd"):
        is_cdrom = False
        f = None
        try:
            f = open(path, 'r')
            if fcntl.ioctl(f, CDROM.CDROM_GET_CAPABILITY) == 0:
                is_cdrom = True
        except: # Any exception implies this is not a CDROM
            pass

        if f is not None:
            f.close()

        if is_cdrom:
            return True

    if os.path.exists("/sys/block/%s/removable" % dev):
        return int(__readOneLineFile__("/sys/block/%s/removable" % dev)) == 1
    else:
        return False

def blockSizeToGBSize(blocks):
    return (long(blocks) * 512) / (1024 * 1024 * 1024)

def blockSizeToMBSize(blocks):
    return (long(blocks) * 512) / (1024 * 1024)

def getHumanDiskSize(blocks):
    gb = blockSizeToGBSize(blocks)
    if gb > 0:
        return "%d GB" % gb
    else:
        return "%d MB" % blockSizeToMBSize(blocks)

def getExtendedDiskInfo(disk, inMb=0):
    return (getDiskDeviceVendor(disk), getDiskDeviceModel(disk),
            inMb and (getDiskDeviceSize(disk)/2048) or getDiskDeviceSize(disk))


def readExtPartitionLabel(partition):
    """Read the ext partition label."""
    rc, out = util.runCmd2(['/sbin/e2label', partition], with_stdout=True)
    if rc == 0:
        label = out.strip()
    else:
        raise Exception("%s is not ext partition" % partition)
    return label

def getMdDeviceName(disk):
    rv, out = util.runCmd2(['mdadm', '--detail', '--export', disk],
                           with_stdout=True)
    for line in out.split("\n"):
        line = line.strip().split('=', 1)
        if line[0] == 'MD_DEVNAME':
            return line[1]

    return disk

def getHumanDiskName(disk):

    # For Multipath nodes return info about 1st slave
    if not disk.startswith("/dev/"):
        disk = '/dev/' + disk
    if isDeviceMapperNode(disk):
        return getHumanDiskName(getDeviceSlaves(disk)[0])
    if is_raid(disk):
        name = getMdDeviceName(disk)
        # mdadm may append an _ followed by a number (e.g. d0_0) to prevent
        # name collisions. Strip it if necessary.
        name = re.match("([^_]*)(_\d+)?$", name).group(1)
        return 'RAID: %s(%s)' % (name, ','.join(map(lambda dev: dev[5:],
                                                    getDeviceSlaves(disk))))

    if disk.startswith('/dev/disk/by-id/'):
        return disk[16:]
    if disk.startswith('/dev/'):
        return disk[5:]
    return disk

# given a list of disks, work out which ones are part of volume
# groups that will cause a problem if we install XE to those disks:
def findProblematicVGs(disks):
    real_disks = map(lambda d: os.path.realpath(d), disks)

    # which disks are the volume groups on?
    vgdiskmap = {}
    tool = LVMTool()
    for pv in tool.pvs:
        if pv['vg_name'] not in vgdiskmap: vgdiskmap[pv['vg_name']] = []
        try:
            device = diskDevice(pv['pv_name'])
        except:
            # CA-35020: whole disk
            device = pv['pv_name']
        vgdiskmap[pv['vg_name']].append(device)

    # for each VG, map the disk list to a boolean list saying whether that
    # disk is in the set we're installing to:
    vgusedmap = {}
    for vg in vgdiskmap:
        vgusedmap[vg] = [disk in real_disks for disk in vgdiskmap[vg]]

    # now, a VG is problematic if it its vgusedmap entry contains a mixture
    # of True and False.  If it's entirely True or entirely False, that's OK:
    problems = []
    for vg in vgusedmap:
        p = False
        for x in vgusedmap[vg]:
            if x != vgusedmap[vg][0]:
                p = True
        if p:
            problems.append(vg)

    return problems

def log_available_disks():
    disks = getQualifiedDiskList()

    # make sure we have discovered at least one disk and
    # at least one network interface:
    if len(disks) == 0:
        logger.log("No disks found on this host.")
    else:
        # make sure that we have enough disk space:
        logger.log("Found disks: %s" % str(disks))
        diskSizes = [getDiskDeviceSize(x) for x in disks]
        diskSizesGB = [blockSizeToGBSize(x) for x in diskSizes]
        logger.log("Disk sizes: %s" % str(diskSizesGB))

        old_dom0disks = filter(lambda x: constants.min_primary_disk_size_old <= x,
                           diskSizesGB)
        if len(old_dom0disks) == 0:
            logger.log("Unable to find a suitable disk (with a size greater than %dGB) to install to." % constants.min_primary_disk_size_old)

        dom0disks = filter(lambda x: constants.min_primary_disk_size <= x,
                           diskSizesGB)
        if len(dom0disks) == 0:
            logger.log("Unable to find a suitable disk (with a size greater than %dGB) to install to." % constants.min_primary_disk_size)

INSTALL_RETAIL = 1
STORAGE_LVM = 1
STORAGE_EXT3 = 2

def probeDisk(device, justInstall=False):
    """Examines device and reports the apparent presence of a XenServer installation and/or related usage
    Returns a tuple (boot, state, storage)

    Where:

        boot is a tuple of True or False and the partition device
        root is a tuple of None, INSTALL_RETAIL and the partition device
        state is a tuple of True or False and the partition device
        storage is a tuple of None, STORAGE_LVM or STORAGE_EXT3 and the partition device
    """

    boot = (False, None)
    root = (None, None)
    state = (False, None)
    storage = (None, None)
    logs = (False, None)
    possible_srs = []

    tool = PartitionTool(device)
    for num, part in tool.iteritems():
        label = None
        part_device = tool._partitionDevice(num)

        if part['id'] == tool.ID_LINUX:
            try:
                label = readExtPartitionLabel(part_device)
            except:
                pass

        if part['id'] == tool.ID_LINUX:
            # probe for retail
            if label and label.startswith('root-'):
                root = (INSTALL_RETAIL, part_device)
                state = (True, part_device)
                if num + 2 in tool.partitions:
                    # George Retail and earlier didn't use the correct id for SRs
                    possible_srs = [num+2]
            elif label and label.startswith('logs-'):
                logs = (True, part_device)
        elif part['id'] == tool.ID_LINUX_LVM:
            if num not in possible_srs:
                possible_srs.append(num)
        elif part['id'] == GPTPartitionTool.ID_EFI_BOOT or part['id'] == GPTPartitionTool.ID_BIOS_BOOT:
            boot = (True, part_device)

    if not justInstall:
        lv_tool = len(possible_srs) and LVMTool()
        for num in possible_srs:
            part_device = tool._partitionDevice(num)

            if lv_tool.isPartitionConfig(part_device):
                state = (True, part_device)
            elif lv_tool.isPartitionSR(part_device):
                pv = lv_tool.deviceToPVOrNone(part_device)
                if pv is not None and pv['vg_name'].startswith(lv_tool.VG_EXT_SR_PREFIX):
                    # odd 'ext3 in an LV' SR
                    storage = (STORAGE_EXT3, part_device)
                else:
                    storage = (STORAGE_LVM, part_device)

    logger.log('Probe of %s found boot=%s root=%s state=%s storage=%s logs=%s' %
                  (device, str(boot), str(root), str(state), str(storage), str(logs)))

    return (boot, root, state, storage, logs)


# Keep track of iscsi disks we have logged into
iscsi_disks = []
# Keep track of NICs reserved for iSCSI boot
ibft_reserved_nics = set()


def get_initiator_name():
    """Return the iSCSI initiator name from the iBFT."""

    with open('%s/initiator/initiator-name' % constants.SYSFS_IBFT_DIR, 'r') as f:
        return f.read().strip()


def is_iscsi(device):
    """Return True if this is an iSCSI device."""

    # If this is a multipath device check whether the first slave is iSCSI
    if use_mpath:
        slaves = getDeviceSlaves(device)
        if slaves:
            device = slaves[0]

    major, minor = getMajMin(device)

    for d in iscsi_disks:
        try:
            if (major, minor) == getMajMin(d):
                return True
        except:
            pass

    return False


def configure_ibft_nic(target_ip, iface, ip, nm, gw):
    prefix = sum([bin(int(i)).count('1') for i in nm.split('.')])
    rv = util.runCmd2(['ip', 'addr', 'add', '%s/%s' % (ip, prefix), 'dev', iface])
    if rv:
        raise RuntimeError('Failed to initialize NIC for iSCSI')

    if netutil.network(ip, nm) == netutil.network(target_ip, nm):
        # Same subnet, don't use the gateway
        rv = util.runCmd2(['ip', 'route', 'add', target_ip, 'dev', iface])
    elif gw:
        rv = util.runCmd2(['ip', 'route', 'add', target_ip, 'dev', iface, 'via', gw])
    else:
        raise RuntimeError('A gateway is needed to initialize NIC for iSCSI')

    if rv:
        raise RuntimeError('Failed to initialize NIC for iSCSI')


# Set up the NICs according to the iBFT. It should be possible to use
# iscsistart -N to do this but that currently doesn't work with NICs which
# support offload (e.g. bnx2) even when offload is not being used.
def setup_ibft_nics():
    mac_map = {}
    netdevs = netutil.scanConfiguration()
    for name in netdevs:
        mac_map[netdevs[name].hwaddr] = name
    logger.log('NET: %s %s' % (repr(netdevs), repr(mac_map)))

    for t in glob.glob(os.path.join(constants.SYSFS_IBFT_DIR, 'target*')):
        with open(os.path.join(t, 'ip-addr'), 'r') as f:
            target_ip = f.read().strip()
        with open(os.path.join(t, 'nic-assoc'), 'r') as f:
            nic_assoc = f.read().strip()

        e_dir = os.path.join(constants.SYSFS_IBFT_DIR, 'ethernet' + nic_assoc)
        with open(os.path.join(e_dir, 'mac'), 'r') as f:
            mac = f.read().strip()
        with open(os.path.join(e_dir, 'ip-addr'), 'r') as f:
            ip = f.read().strip()
        try:
            with open(os.path.join(e_dir, 'gateway'), 'r') as f:
                gw = f.read().strip()
        except IOError as err:
            if err.errno == errno.ENOENT:
                gw = None
            else:
                raise
        with open(os.path.join(e_dir, 'subnet-mask'), 'r') as f:
            nm = f.read().strip()
        with open(os.path.join(e_dir, 'flags'), 'r') as f:
            flags = int(f.read().strip())
            assert (flags & 3) == 3

        if mac not in mac_map:
            raise RuntimeError('Found mac %s in iBFT but cannot find matching NIC' % mac)

        configure_ibft_nic(target_ip, mac_map[mac], ip, nm, gw)
        ibft_reserved_nics.add(mac_map[mac])


def dump_ibft():
    logger.log("Dump iBFT:")
    for path, dirs, files in os.walk('/sys/firmware/ibft'):
        for item in dirs:
            logger.log(os.path.join(path, item) + '/')
        for item in files:
            item =  os.path.join(path, item)
            with open(item, 'r') as f:
                data = f.read()
            logger.log('%s %s' % (item, repr(data)))
    logger.log("End of iBFT dump")


def write_iscsi_records(mounts, primary_disk):
    record = []
    node_name = node_address = node_port = None

    rv, out = util.runCmd2(['iscsistart', '-f'], with_stdout=True)
    if rv:
        raise Exception('Invalid iSCSI record')

    for line in out.split('\n'):
        line = line.strip()
        if not line:
            continue

        if line.startswith('node.name = '):
            node_name = line.split()[2]
        if line.startswith('node.conn[0].address = '):
            node_address = line.split()[2]
        if line.startswith('node.conn[0].port = '):
            node_port = line.split()[2]
        if line == '# END RECORD':
            if node_name is None or node_address is None or node_port is None:
                raise Exception('Invalid iSCSI record')

            # Ensure that the session does not get logged out during shutdown
            record.append('node.startup = onboot')
            # iscsistart hardcodes the target portal group tag to 1
            record.append('node.tpgt = 1')
            if isDeviceMapperNode(primary_disk):
                record.append('%s = %d\n' % ('node.session.timeo.replacement_timeout',
                                             constants.MPATH_ISCSI_TIMEOUT))
            record.append(line)

            path = os.path.join(mounts['root'], constants.ISCSI_NODES,
                                node_name, '%s,%s,1' % (node_address, node_port))
            os.makedirs(path)
            with open(os.path.join(path, 'default'), 'w') as f:
                f.write('\n'.join(record) + '\n')
            record = []
            node_name = node_address = node_port = None
            continue

        record.append(line)

    if record:
        raise Exception('Invalid iSCSI record')


def process_ibft(ui, interactive):
    """Process the iBFT.

    Bring up any disks that the iBFT says should be attached, and reserve the
    NICs that it says should be used for iSCSI.
    """

    util.runCmd2([ '/sbin/iscsiadm', '-k', '0'])
    rv = util.runCmd2(['iscsid'])
    if rv:
        raise RuntimeError('Failed to start iscsid')

    nics = set()
    targets = 0
    rv, out = util.runCmd2(['iscsistart', '-f'], with_stdout=True)
    if rv:
        logger.log("process_ibft: No valid iBFT found.")

        # Dump iBFT state for debugging
        dump_ibft()

        return
    for line in out.split('\n'):
        m = re.match('iface.net_ifacename = (.*)$', line.strip())
        if m:
            nics.add(m.group(1))
        m = re.match(r'node.conn\[\d+\].address = ', line.strip())
        if m:
            targets += 1

    # Do nothing if the iBFT contains no valid targets
    if targets == 0:
        logger.log("process_ibft: No valid target configs found in iBFT")
        return

    # If interactive, ask user if he wants to proceed
    if ui and interactive:
        msg = \
            "Found iSCSI Boot Firmware Table\n\nAttach to disks specified in iBFT?\n\n" \
            "This will reserve %s for iSCSI disk access.  Reserved NICs are not available " \
            "for use as the management interface or for use by virtual machines."  % " and ".join(sorted(nics))
        button = ButtonChoiceWindowEx(ui.screen, "Attach iSCSI disks" , msg, ['Yes', 'No'], width=60)
        if button == 'no':
            return

    setup_ibft_nics()

    # Attach disks
    rv = util.runCmd2(['iscsistart', '-b'])
    if rv:
        raise RuntimeError('Failed to attach iSCSI target disk(s)')

    util.runCmd2(util.udevsettleCmd())
    time.sleep(5)

    rv, out = util.runCmd2([ 'iscsiadm', '-m', 'session', '-P', '3' ],
                           with_stdout=True)
    if rv:
        raise RuntimeError('Failed to find attached disks')
    for line in out.split('\n'):
        m = re.match(r'\s*Attached scsi disk (\w+)\s+.*$', line)
        if m:
            iscsi_disks.append('/dev/' + m.group(1))

    logger.log('process_ibft: iSCSI Disks: %s' % (str(iscsi_disks),))
    logger.log('process_ibft: Reserved NICs: %s' % (str(list(ibft_reserved_nics)),))


def release_ibft_disks():
    if util.pidof('iscsid'):
        util.runCmd2([ '/sbin/iscsiadm', '-m', 'session', '-u'])
        util.runCmd2([ '/sbin/iscsiadm', '-k', '0'])
        iscsi_disks = []


def is_raid(disk):
    return disk in getMdNodes()


def dev_from_devpath(devpath):
    """Returns the dev number of the device as a tuple."""

    devno = os.stat(devpath).st_rdev
    return os.major(devno), os.minor(devno)


def dev_from_sysfs(path):
    """Returns the dev number as a tuple from a sysfs entry."""

    with open('%s/dev' % path, 'r') as f:
        return tuple(map(int, f.read().strip().split(':')))


# The logic for this function is based on sysfs_devno_to_wholedisk in
# util-linux. See that function for reasoning.
def parentdev_from_devpath(devpath):
    """Returns the dev number of the parent device, or None if there isn't
    one."""

    try:
        devno = os.stat(devpath).st_rdev
        major = os.major(devno)
        minor = os.minor(devno)
        syspath = '/sys/dev/block/%d:%d' % (major, minor)

        partitionpath = syspath + '/partition'
        if os.path.exists(partitionpath):
            linkpath = os.path.realpath(syspath)
            parent = os.path.dirname(linkpath)
            return dev_from_sysfs(parent)
        else:
            dm_uuidpath = syspath + '/dm/uuid'
            if os.path.exists(dm_uuidpath):
                with open(dm_uuidpath, 'r') as f:
                    dm_uuid = f.read().strip()
                if re.match('part[0-9+]-', dm_uuid):
                    parent = os.listdir(syspath + '/slaves')[0]
                    return dev_from_sysfs('%s/slaves/%s' % (syspath, parent))
    except Exception as e:
        logger.logException(e)

    # If there is no parent of the parent cannot be determined...
    return None
