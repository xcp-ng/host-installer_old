# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this 
# copyrighted material is governed by and subject to terms and conditions 
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or 
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Manage product installations
#
# written by Andrew Peace

import os

import diskutil
import util
import netutil
from netinterface import *
import constants
import version
import re
import stat
import xelogging
import repository
from disktools import *
import hardware
import xcp
import xcp.bootloader as bootloader
from xcp.version import *
import xml.dom.minidom
import simplejson as json

class SettingsNotAvailable(Exception):
    pass

THIS_PLATFORM_VERSION = Version.from_string(version.PLATFORM_VERSION)
XENSERVER_6_0_0 = Version([1, 0, 99]) # Platform version

class ExistingInstallation:
    def __init__(self, primary_disk, boot_device, state_device):
        self.primary_disk = primary_disk
        self.boot_device = boot_device
        self.state_device = state_device
        self.state_prefix = ''
        self.settings = None
        self.root_fs = None
        self._boot_fs = None
        self.boot_fs_mount = None

    def __str__(self):
        return "%s %s" % (
            self.visual_brand, self.visual_version)

    def mount_state(self):
        """ Mount main state partition on self.state_fs. """
        self.state_fs = util.TempMount(self.state_device, 'state-', )

    def unmount_state(self):
        self.state_fs.unmount()
        self.state_fs = None

    def join_state_path(self, *path):
        """ Construct an absolute path to a file in the main state partition. """
        return os.path.join(self.state_fs.mount_point, self.state_prefix, *path)

    def getInventoryValue(self, k):
        return self.inventory[k]

    def isUpgradeable(self):
        self.mount_state()
        try:
            # CA-38459: handle missing firstboot directory e.g. Rio
            if not os.path.exists(self.join_state_path('etc/firstboot.d/state')):
                return False
            firstboot_files = [ f for f in os.listdir(self.join_state_path('etc/firstboot.d')) \
                                if f[0].isdigit() and os.stat(self.join_state_path('etc/firstboot.d', f))[stat.ST_MODE] & stat.S_IXUSR ]
            missing_state_files = filter(lambda x: not os.path.exists(self.join_state_path('etc/firstboot.d/state', x)), firstboot_files)

            result = (len(missing_state_files) == 0)
            if not result:
                xelogging.log('Upgradeability test failed:')
                xelogging.log('  Firstboot:     '+', '.join(firstboot_files))
                xelogging.log('  Missing state: '+', '.join(missing_state_files))
        finally:
            self.unmount_state()
        return result

    def settingsAvailable(self):
        try:
            self.readSettings()
        except SettingsNotAvailable, text:
            xelogging.log("Settings unavailable: %s" % text)
            return False
        except Exception, e:
            xelogging.log("Settings unavailable: unhandled exception")
            xelogging.log_exception(e)
            return False
        else:
            return True

    def _readSettings(self):
        """ Read settings from the installation, returns a results dictionary. """
        
        results = { 'host-config': {} }

        self.mount_state()
        try:

            # timezone:
            tz = None
            clock_file = self.join_state_path('etc/localtime')
            if os.path.islink(clock_file):
                tzfile = os.path.realpath(clock_file)
                if '/usr/share/zoneinfo/' in tzfile:
                    _, tz = tzfile.split('/usr/share/zoneinfo/', 1)
            if not tz:
                # No timezone found: 
                # Supply a default and for interactive installs prompt the user.
                xelogging.log('No timezone configuration found.')
                results['request-timezone'] = True
                tz = "Europe/London"
            results['timezone'] = tz

            # hostname.  We will assume one was set anyway and thus write
            # it back into the new filesystem.  If one wasn't set then this
            # will be localhost.localdomain, in which case the old behaviour
            # will persist anyway:
            fd = open(self.join_state_path('etc/sysconfig/network'), 'r')
            lines = fd.readlines()
            fd.close()
            for line in lines:
                if line.startswith('HOSTNAME='):
                    results['manual-hostname'] = (True, line[9:].strip())

            if os.path.exists(self.join_state_path('etc/hostname')):
                fd = open(self.join_state_path('etc/hostname'), 'r')
                line = fd.readline()
                results['manual-hostname'] = (True, line.strip())
                fd.close()

            if not results.has_key('manual-hostname'):
                results['manual-hostname'] = (False, None)

            # nameservers:
            domain = None
            if not os.path.exists(self.join_state_path('etc/resolv.conf')):
                results['manual-nameservers'] = (False, None)
            else:
                ns = []
                fd = open(self.join_state_path('etc/resolv.conf'), 'r')
                lines = fd.readlines()
                fd.close()
                for line in lines:
                    if line.startswith("nameserver "):
                        ns.append(line[11:].strip())
                    elif line.startswith("domain "):
                        domain = line[8:].strip()
                    elif line.startswith("search "):
                        domain = line.split()[1]
                results['manual-nameservers'] = (True, ns)

            # ntp servers:
            fd = open(self.join_state_path('etc/ntp.conf'), 'r')
            lines = fd.readlines()
            fd.close()
            ntps = []
            for line in lines:
                if line.startswith("server "):
                    ntps.append(line[7:].strip())
            results['ntp-servers'] = ntps

            # keyboard:
            keyboard_dict = {}
            keyboard_file = self.join_state_path('etc/sysconfig/keyboard')
            if os.path.exists(keyboard_file):
                keyboard_dict = util.readKeyValueFile(keyboard_file)
            keyboard_file = self.join_state_path('etc/vconsole.conf')
            if os.path.exists(keyboard_file):
                keyboard_dict.update(util.readKeyValueFile(keyboard_file))
            if 'KEYMAP' in keyboard_dict:
                results['keymap'] = keyboard_dict['KEYMAP']
            elif 'KEYTABLE' in keyboard_dict:
                results['keymap'] = keyboard_dict['KEYTABLE']
            # Do not error here if no keymap configuration is found.
            # This enables upgrade to still carry state on hosts without
            # keymap configured: 
            # A default keymap is assigned in the backend of this installer.
            if not results.has_key('keymap'):
                xelogging.log('No existing keymap configuration found.')

            # root password:
            fd = open(self.join_state_path('etc/passwd'), 'r')
            root_pwd = None
            for line in fd:
                pwent = line.split(':')
                if pwent[0] == 'root':
                    root_pwd = pwent[1]
                    break
            fd.close()
            if len(root_pwd) == 1:
                root_pwd = None
                try:
                    fd = open(self.join_state_path('etc/shadow'), 'r')
                    for line in fd:
                        pwent = line.split(':')
                        if pwent[0] == 'root':
                            root_pwd = pwent[1]
                            break
                    fd.close()
                except:
                    pass

            if not root_pwd:
                raise SettingsNotAvailable, "no root password found"
            results['root-password'] = ('pwdhash', root_pwd)

            # don't care about this too much.
            results['time-config-method'] = 'ntp'

            # read network configuration.  We only care to find out what the
            # management interface is, and what its configuration was.
            # The dev -> MAC mapping for other devices will be preserved in the
            # database which is available in time for everything except the
            # management interface.
            mgmt_iface = self.getInventoryValue('MANAGEMENT_INTERFACE')

            networkdb_path = constants.NETWORK_DB
            if not os.path.exists(self.join_state_path(networkdb_path)):
                networkdb_path = constants.OLD_NETWORK_DB
            dbcache_path = constants.DBCACHE
            if not os.path.exists(self.join_state_path(dbcache_path)):
                dbcache_path = constants.OLD_DBCACHE

            if not mgmt_iface:
                xelogging.log('No existing management interface configuration found.')
            elif os.path.exists(self.join_state_path(networkdb_path)):
                networkd_db = constants.NETWORKD_DB
                if not os.path.exists(self.join_state_path(networkd_db)):
                    networkd_db = constants.OLD_NETWORKD_DB

                def fetchIfaceInfoFromNetworkdbAsDict(bridge, iface=None):
                    args = ['chroot', self.state_fs.mount_point, '/'+networkd_db, '-bridge', bridge]
                    if iface:
                        args.extend(['-iface', iface])
                    rv, out = util.runCmd2(args, with_stdout = True)
                    d = {}
                    for line in (x.strip() for x in out.split('\n') if len(x.strip())):
                        for key_value in line.split(" "):
                            var = key_value.split('=', 1)
                            d[var[0]] = var[1]
                    return d

                d = fetchIfaceInfoFromNetworkdbAsDict(mgmt_iface, mgmt_iface)
                # For mgmt on tagged vlan, networkdb output has no value for
                # 'interfaces' but instead has 'parent' specified. We need
                # to fetch 'interfaces' of parent and use for mgmt bridge.
                if not d.get('interfaces') and 'parent' in d:
                    p = fetchIfaceInfoFromNetworkdbAsDict(d['parent'])
                    d['interfaces'] = p['interfaces']

                results['net-admin-bridge'] = mgmt_iface
                results['net-admin-interface'] = d.get('interfaces').split(',')[0]

                if_hwaddr = netutil.getHWAddr(results['net-admin-interface'])

                vlan = int(d['vlan']) if 'vlan' in d else None
                proto = d.get('mode')
                if proto == 'static':
                    ip = d.get('ipaddr')
                    netmask = d.get('netmask')
                    gateway = d.get('gateway')
                    dns = d.get('dns', '').split(',')
                    if ip and netmask:
                        results['net-admin-configuration'] = NetInterface(NetInterface.Static, if_hwaddr, ip, netmask, gateway, dns, vlan=vlan)
                elif proto == 'dhcp':
                    results['net-admin-configuration'] = NetInterface(NetInterface.DHCP, if_hwaddr, vlan=vlan)
                else:
                    results['net-admin-configuration'] = NetInterface(None, if_hwaddr, vlan=vlan)

                protov6 = d.get('modev6')
                if protov6 == 'static':
                    ipv6 = d.get('ipaddrv6')
                    gatewayv6 = d.get('gatewayv6')
                    if ipv6:
                        results['net-admin-configuration'].addIPv6(NetInterface.Static, ipv6, gatewayv6)
                elif protov6 == 'dhcp':
                    results['net-admin-configuration'].addIPv6(NetInterface.DHCP)
                elif protov6 == 'autoconf':
                    results['net-admin-configuration'].addIPv6(NetInterface.Autoconf)
                    
            elif os.path.exists(self.join_state_path(dbcache_path)):
                def getText(nodelist):
                    rc = ""
                    for node in nodelist:
                        if node.nodeType == node.TEXT_NODE:
                            rc = rc + node.data
                    return rc.strip().encode()
                
                xmldoc = xml.dom.minidom.parse(self.join_state_path(dbcache_path))

                pif_uid = None
                for node in xmldoc.documentElement.childNodes:
                    if node.nodeType == node.ELEMENT_NODE and node.tagName == 'network':
                        network = node
                    else:
                        continue
                    # CA-50971: handle renamed networks in MNR
                    if len(network.getElementsByTagName('bridge')) == 0 or \
                       len(network.getElementsByTagName('PIFs')) == 0 or \
                       len(network.getElementsByTagName('PIFs')[0].getElementsByTagName('PIF')) == 0:
                        continue
                
                    if getText(network.getElementsByTagName('bridge')[0].childNodes) == mgmt_iface:
                        pif_uid = getText(network.getElementsByTagName('PIFs')[0].getElementsByTagName('PIF')[0].childNodes)
                        break
                if pif_uid:
                    for node in xmldoc.documentElement.childNodes:
                        if node.nodeType == node.ELEMENT_NODE and node.tagName == 'pif':
                            pif = node
                        else:
                            continue
                        if pif.getAttribute('ref') == pif_uid:
                            results['net-admin-interface'] = getText(pif.getElementsByTagName('device')[0].childNodes)
                            results['net-admin-bridge'] = mgmt_iface
                            results['net-admin-configuration'] = NetInterface.loadFromPif(pif)
                            break
            else:
                for cfile in filter(lambda x: True in [x.startswith(y) for y in ['ifcfg-eth', 'ifcfg-bond']], \
                                   os.listdir(self.join_state_path(constants.NET_SCR_DIR))):
                    devcfg = util.readKeyValueFile(self.join_state_path(constants.NET_SCR_DIR, cfile), strip_quotes = False)
                    if devcfg.has_key('DEVICE') and devcfg.has_key('BRIDGE') and devcfg['BRIDGE'] == mgmt_iface:
                        brcfg = util.readKeyValueFile(self.join_state_path(constants.NET_SCR_DIR, 'ifcfg-'+devcfg['BRIDGE']), strip_quotes = False)
                        results['net-admin-interface'] = devcfg['DEVICE']
                        results['net-admin-bridge'] = devcfg['BRIDGE']

                        # get hardware address if it was recorded, otherwise look it up:
                        if devcfg.has_key('HWADDR'):
                            hwaddr = devcfg['HWADDR']
                        elif devcfg.has_key('MACADDR'):
                            # our bonds have a key called MACADDR instead
                            hwaddr = devcfg['MACADDR']
                        else:
                            hwaddr = netutil.getHWAddr(devcfg['DEVICE'])

                        ifcfg = NetInterface.loadFromIfcfg(self.join_state_path(constants.NET_SCR_DIR, 'ifcfg-'+devcfg['BRIDGE']))
                        if not ifcfg.hwaddr:
                            ifcfg.hwaddr = hwaddr
                        if ifcfg.isStatic() and not ifcfg.domain and domain:
                            ifcfg.domain = domain
                        results['net-admin-configuration'] = ifcfg
                        break

            repo_list = []
            if os.path.exists(self.join_state_path(constants.INSTALLED_REPOS_DIR)):
                try:
                    for repo_id in os.listdir(self.join_state_path(constants.INSTALLED_REPOS_DIR)):
                        try:
                            repo = repository.LegacyRepository(repository.FilesystemAccessor(self.join_state_path(constants.INSTALLED_REPOS_DIR, repo_id)))
                            if repo.hidden() != "true":
                                repo_list.append((repo.identifier(), repo.name(), (repo_id != constants.MAIN_REPOSITORY_NAME)))
                        except repository.RepoFormatError:
                            # probably pre-XML format
                            repo = open(self.join_state_path(constants.INSTALLED_REPOS_DIR, repo_id, repository.LegacyRepository.REPOSITORY_FILENAME))
                            repo_id = repo.readline().strip()
                            repo_name = repo.readline().strip()
                            repo.close()
                            repo_list.append((repo_id, repo_name, (repo_id != constants.MAIN_REPOSITORY_NAME)))
                except Exception, e:
                    xelogging.log('Scan for driver disks failed:')
                    xelogging.log_exception(e)

            results['repo-list'] = repo_list

            results['ha-armed'] = False
            try:
                db_path = "var/lib/xcp/local.db"
                if not os.path.exists(self.join_state_path(db_path)):
                    db_path = "var/xapi/local.db"
                db = open(self.join_state_path(db_path), 'r')
                if db.readline().find('<row key="ha.armed" value="true"') != -1:
                    results['ha-armed'] = True
                db.close()
            except:
                pass

            try:
                network_conf = open(self.join_state_path("etc/xensource/network.conf"), 'r')
                network_backend = network_conf.readline().strip()
                network_conf.close()

                if network_backend == constants.NETWORK_BACKEND_BRIDGE:
                    results['network-backend'] = constants.NETWORK_BACKEND_BRIDGE
                elif network_backend in [constants.NETWORK_BACKEND_VSWITCH, constants.NETWORK_BACKEND_VSWITCH_ALT]:
                    results['network-backend'] = constants.NETWORK_BACKEND_VSWITCH
                else:
                    raise SettingsNotAvailable, "unknown network backend %s" % network_backend
            except:
                pass

            results['master'] = None
            try:
                pt = open(self.join_state_path("etc/xensource/ptoken"), 'r')
                results['pool-token'] = pt.readline().strip()
                pt.close()
                pc = open(self.join_state_path("etc/xensource/pool.conf"), 'r')
                line = pc.readline().strip()
                if line.startswith('slave:'):
                    results['master'] = line[6:]
                pc.close()
            except:
                pass

        finally:
            self.unmount_state()

        # read bootloader config to extract various settings
        try:
            # Boot device
            self.mount_boot()
            boot_config = bootloader.Bootloader.loadExisting(self.boot_fs_mount)

            # Serial console
            if boot_config.serial:
                results['serial-console'] = hardware.SerialPort(boot_config.serial['port'],
                                                                baud = str(boot_config.serial['baud']))
            results['bootloader-location'] = boot_config.location
            if boot_config.default != 'upgrade':
                results['boot-serial'] = (boot_config.default == 'xe-serial')

            # Subset of hypervisor arguments
            xen_args = boot_config.menu[boot_config.default].getHypervisorArgs()

            #   - cpuid_mask
            results['host-config']['xen-cpuid-masks'] = filter(lambda x: x.startswith('cpuid_mask'), xen_args)

            #   - dom0_mem
            dom0_mem_arg = filter(lambda x: x.startswith('dom0_mem'), xen_args)
            (dom0_mem, dom0_mem_min, dom0_mem_max) = xcp.dom0.parse_mem(dom0_mem_arg[0])
            if dom0_mem:
                results['host-config']['dom0-mem'] = dom0_mem / 1024 / 1024
        except:
            pass
        self.unmount_boot()

        return results

    def mount_boot(self, ro = True):
        opts = None
        if ro:
            opts = ['ro']
        self._boot_fs = util.TempMount(self.boot_device, 'boot', opts, 'ext3')
        self.boot_fs_mount = self._boot_fs.mount_point

    def unmount_boot(self):
        if self.boot_fs:
            self._boot_fs.unmount()
            self._boot_fs = None
            self.boot_fs_mount = None

    def readSettings(self):
        if not self.settings:
            self.settings = self._readSettings()
        return self.settings


class ExistingRetailInstallation(ExistingInstallation):
    def __init__(self, primary_disk, boot_device, root_device, state_device, storage):
        self.variant = 'Retail'
        ExistingInstallation.__init__(self, primary_disk, boot_device, state_device)
        self.root_device = root_device
        self._boot_fs_mounted = False
        self.readInventory()

    def __repr__(self):
        return "<ExistingRetailInstallation: %s on %s>" % (str(self), self.root_device)

    def mount_root(self, ro = True, boot_device = None):
        opts = None
        if ro:
            opts = ['ro']
        self.root_fs = util.TempMount(self.root_device, 'root', opts, 'ext3', boot_device = boot_device)

    def unmount_root(self):
        if self.root_fs:
            self.root_fs.unmount()
            self.root_fs = None

    # Because EFI boot stores the bootloader configuration on the ESP, mount
    # it at its usual location if necessary so that the configuration is found.
    def mount_boot(self, ro = True):
        self.mount_root(ro = ro, boot_device = self.boot_device)
        self.boot_fs_mount = self.root_fs.mount_point

    def unmount_boot(self):
        self.unmount_root()
        self.boot_fs_mount = None

    def readInventory(self):
        self.mount_root()
        try:
            self.inventory = util.readKeyValueFile(os.path.join(self.root_fs.mount_point,
                                                                constants.INVENTORY_FILE),
                                                   strip_quotes = True)
            self.build = self.inventory['BUILD_NUMBER']
            self.version = Version.from_string("%s-%s" % (self.inventory['PLATFORM_VERSION'],
                                                          self.build))
            if 'PRODUCT_NAME' in self.inventory:
                self.name = self.inventory['PRODUCT_NAME']
                self.brand = self.inventory['PRODUCT_BRAND']
            else:
                self.name = self.inventory['PLATFORM_NAME']
                self.brand = self.inventory['PLATFORM_NAME']

            if 'OEM_BRAND' in self.inventory:
                self.oem_brand = self.inventory['OEM_BRAND']
                self.visual_brand = self.oem_brand
            else:
                self.visual_brand = self.brand
            if 'OEM_VERSION' in self.inventory:
                self.oem_version = self.inventory['OEM_VERSION']
                self.visual_version = "%s-%s" % (self.inventory['OEM_VERSION'],
                                                 self.build)
            else:
                self.visual_version = "%s-%s" % (self.inventory['PRODUCT_VERSION'],
                                                 self.build)
        finally:
            self.unmount_root()

class XenServerBackup:
    def __init__(self, part, mnt):
        self.partition = part
        self.inventory = util.readKeyValueFile(os.path.join(mnt, constants.INVENTORY_FILE), strip_quotes = True)
        self.build = self.inventory['BUILD_NUMBER']
        self.version = Version.from_string("%s-%s" % (self.inventory['PLATFORM_VERSION'],
                                                      self.build))
        if 'PRODUCT_NAME' in self.inventory:
            self.name = self.inventory['PRODUCT_NAME']
            self.brand = self.inventory['PRODUCT_BRAND']
        else:
            self.name = self.inventory['PLATFORM_NAME']
            self.brand = self.inventory['PLATFORM_NAME']

        if 'OEM_BRAND' in self.inventory:
            self.oem_brand = self.inventory['OEM_BRAND']
            self.visual_brand = self.oem_brand
        else:
            self.visual_brand = self.brand
        if 'OEM_VERSION' in self.inventory:
            self.oem_version = self.inventory['OEM_VERSION']
            self.visual_version = "%s-%s" % (self.inventory['OEM_VERSION'], self.build)
        else:
            self.visual_version = "%s-%s" % (self.inventory['PRODUCT_VERSION'], self.build)

        if self.inventory['PRIMARY_DISK'].startswith('/dev/md_'):
            # Handle restoring an installation using a /dev/md_* path
            self.root_disk = os.path.realpath(self.inventory['PRIMARY_DISK'].replace('md_', 'md/') + '_0')
        else:
            self.root_disk = diskutil.partitionFromId(self.inventory['PRIMARY_DISK'])
            self.root_disk = getMpathMasterOrDisk(self.root_disk)

    def __str__(self):
        return "%s %s" % (
            self.visual_brand, self.visual_version)

    def __repr__(self):
        return "<XenServerBackup: %s on %s>" % (str(self), self.partition)

def findXenSourceBackups():
    """Scans the host and find partitions containing backups of XenSource
    products.  Returns a list of device node paths to partitions containing
    said backups. """
    partitions = diskutil.getQualifiedPartitionList()
    backups = []

    for p in partitions:
        b = None
        try:
            b = util.TempMount(p, 'backup-', ['ro'], 'ext3')
            if os.path.exists(os.path.join(b.mount_point, '.xen-backup-partition')):
                backups.append(XenServerBackup(p, b.mount_point))
        except:
            pass
        if b:
            b.unmount()

    return backups

def findXenSourceProducts():
    """Scans the host and finds XenSource product installations.
    Returns list of ExistingInstallation objects.

    Currently requires supervisor privileges due to mounting
    filesystems."""

    installs = []

    for disk in diskutil.getQualifiedDiskList():
        (boot, root, state, storage, logs) = diskutil.probeDisk(disk)

        inst = None
        try:
            if root[0] == diskutil.INSTALL_RETAIL:
                inst = ExistingRetailInstallation(disk, boot[1], root[1], state[1], storage)
        except Exception, e:
            xelogging.log("A problem occurred whilst scanning for existing installations:")
            xelogging.log_exception(e)
            xelogging.log("This is not fatal.  Continuing anyway.")

        if inst:
            xelogging.log("Found an installation: %s on %s" % (str(inst), disk))
            installs.append(inst)

    return installs

def readInventoryFile(filename):
    return util.readKeyValueFile(filename, strip_quotes = True)

def find_installed_products():
    try:
        installed_products = findXenSourceProducts()
    except Exception, e:
        xelogging.log("A problem occurred whilst scanning for existing installations:")
        xelogging.log_exception(e)
        xelogging.log("This is not fatal.  Continuing anyway.")
        installed_products = []
    return installed_products
            
