# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Network interface management utils
#
# written by Andrew Peace

import os
import diskutil
import util
import re
import subprocess
import time
import errno
from xcp import logger
from xcp.net.biosdevname import all_devices_all_names
from socket import inet_ntoa
from struct import pack

class NIC:
    def __init__(self, nic_dict):
        self.name = nic_dict.get("Kernel name", "")
        self.hwaddr = nic_dict.get("Assigned MAC", "").lower()
        self.pci_string = nic_dict.get("Bus Info", "").lower()
        self.driver = "%s (%s)" % (nic_dict.get("Driver", ""),
                                   nic_dict.get("Driver version", ""))
        self.smbioslabel = nic_dict.get("SMBIOS Label", "")
        self.bond_mode = nic_dict.get("Bond mode", None)
        self.bond_members = nic_dict.get("Bond mode", None)

    def __repr__(self):
        return "<NIC: %s (%s)>" % (self.name, self.hwaddr)

def scanConfiguration():
    """ Returns a dictionary of string -> NIC with a snapshot of the NIC
    configuration.

    Filter out any NICs that have been reserved by the iBFT for use
    with boot time iSCSI targets.  (iBFT = iSCSI Boot Firmware Tables.)
    This is because we cannot use NICs that are used to access iSCSI
    LUNs for other purposes e.g. XenServer Management.
    """
    conf = {}
    nics = []

    for nif in getNetifList():
        if nif not in diskutil.ibft_reserved_nics:
            nics.append(nif)

    for nic in all_devices_all_names().values():
        name = nic.get("Kernel name", "")
        if name in nics:
            conf[name] = NIC(nic)

    return conf

def getNetifList(include_vlan=False):
    all = os.listdir("/sys/class/net")

    def ethfilter(interface, include_vlan):
        return interface.startswith("eth") and (interface.isalnum() or
                                    (include_vlan and "." in interface))

    def rankValue(ethx):
        iface, vlan = splitInterfaceVlan(ethx)
        return (int(iface.strip('eth'))*10000 + (int(vlan) if vlan else -1))

    relevant = filter(lambda x: ethfilter(x, include_vlan), all)
    relevant.sort(lambda l, r: rankValue(l) - rankValue(r))
    return relevant

# writes an 'interfaces' style file given a network configuration object list
def writeDebStyleInterfaceFile(configuration):
    filename = '/etc/network/interfaces'
    outfile = open(filename, 'w')

    outfile.write("auto lo\n")
    outfile.write("iface lo inet loopback\n")

    for iface in configuration:
        configuration[iface].writeDebStyleInterface(iface, outfile)

    outfile.close()

def writeRHStyleInterfaceFiles(configuration):
    for iface in configuration:
        configuration[iface].writeRHStyleInterface(iface)

def writeNetInterfaceFiles(configuration):
    if os.path.isfile('/etc/sysconfig/network-scripts/ifcfg-lo'):
        writeRHStyleInterfaceFiles(configuration)
    else:
        writeDebStyleInterfaceFile(configuration)

# writes DNS server entries to a resolver file given a network configuration object
# list
def writeResolverFile(configuration, filename):
    outfile = open(filename, 'a')

    for iface in configuration:
        settings = configuration[iface]
        if settings.isStatic() and settings.dns:
            if settings.dns:
                for server in settings.dns:
                    outfile.write("nameserver %s\n" % server)
            if settings.domain:
                outfile.write("search %s\n" % settings.domain)

    outfile.close()

interface_up = {}

# simple wrapper for calling the local ifup script:
def splitInterfaceVlan(interface):
    if "." in interface:
        return interface.split(".", 1)
    return interface, None

def ifup(interface):
    device, vlan = splitInterfaceVlan(interface)
    assert device in getNetifList()
    interface_up[interface] = True
    return util.runCmd2(['ifup', interface])

def ifdown(interface):
    if interface in interface_up:
        del interface_up[interface]
    return util.runCmd2(['ifdown', interface])

def ipaddr(interface):
    rc, out = util.runCmd2(['ip', 'addr', 'show', interface], with_stdout=True)
    if rc != 0:
        return None
    inets = filter(lambda x: 'inet ' in x, out.split("\n"))
    if len(inets) == 1:
        m = re.search(r'inet (\S+)/', inets[0])
        if m:
            return m.group(1)
    return None

# work out if an interface is up:
def interfaceUp(interface):
    rc, out = util.runCmd2(['ip', 'addr', 'show', interface], with_stdout=True)
    if rc != 0:
        return False
    inets = filter(lambda x: x.startswith("    inet "), out.split("\n"))
    return len(inets) == 1

# work out if a link is up:
def linkUp(interface):
    linkUp = None

    try:
        fh = open("/sys/class/net/%s/operstate" % interface)
        state = fh.readline().strip()
        linkUp = (state == 'up')
        fh.close()
    except IOError:
        pass
    return linkUp

def setAllLinksUp():
    subprocs = []

    for nif in getNetifList():
        if nif not in diskutil.ibft_reserved_nics:
            subprocs.append(subprocess.Popen(['ip', 'link', 'set', nif, 'up'], close_fds=True))

    while None in map(lambda x: x.poll(), subprocs):
        time.sleep(1)

def networkingUp():
    rc, out = util.runCmd2(['ip', 'route'], with_stdout=True)
    if rc == 0 and len(out.split('\n')) > 2:
        return True
    return False

# make a string to help users identify a network interface:
def getPCIInfo(interface):
    interface, vlan = splitInterfaceVlan(interface)
    info = "<Information unknown>"
    devpath = os.path.realpath('/sys/class/net/%s/device' % interface)
    slot = devpath[len(devpath) - 7:]

    rc, output = util.runCmd2(['lspci', '-i', '/usr/share/misc/pci.ids', '-s', slot], with_stdout=True)

    if rc == 0:
        info = output.strip('\n')

    cur_if = None
    pipe = subprocess.Popen(['biosdevname', '-d'], bufsize=1, stdout=subprocess.PIPE)
    for line in pipe.stdout:
        l = line.strip('\n')
        if l.startswith('Kernel name'):
            cur_if = l[13:]
        elif l.startswith('PCI Slot') and cur_if == interface and l[16:] != 'embedded':
            info += "\nSlot "+l[16:]
    pipe.wait()

    return info

def getDriver(interface):
    interface, vlan = splitInterfaceVlan(interface)
    return os.path.basename(os.path.realpath('/sys/class/net/%s/device/driver' % interface))

def __readOneLineFile__(filename):
    f = open(filename)
    value = f.readline().strip('\n')
    f.close()
    return value

def getHWAddr(iface):
    try:
        return __readOneLineFile__('/sys/class/net/%s/address' % iface)
    except IOError as e:
        if e.errno == errno.ENOENT:
            return None
        raise

def valid_hostname(x, emptyValid=False, fqdn=False):
    if emptyValid and x == '':
        return True
    if fqdn:
        return re.match('^[a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?)*$', x) is not None
    else:
        return re.match('^[a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?$', x) is not None

def valid_vlan(vlan):
    if not re.match('^\d+$', vlan):
        return False
    if int(vlan)<1 or int(vlan)>=4095:
        return False
    return True

def valid_ip_addr(addr):
    if not re.match('^\d+\.\d+\.\d+\.\d+$', addr):
        return False
    els = addr.split('.')
    if len(els) != 4:
        return False
    for el in els:
        if int(el) > 255:
            return False
    return True

def network(ipaddr, netmask):
    ip = map(int,ipaddr.split('.',3))
    nm = map(int,netmask.split('.',3))
    nw = map(lambda i: ip[i] & nm[i], range(4))
    return ".".join(map(str,nw))

def prefix2netmask(mask):
    bits = 0
    for i in xrange(32-mask, 32):
        bits |= (1 << i)
    return inet_ntoa(pack('>I', bits))

class NetDevices:
    def __init__(self):
        self.netdev = []
        details = {}

        pipe = subprocess.Popen(['biosdevname', '-d'], bufsize=1, stdout=subprocess.PIPE)
        for line in pipe.stdout:
            l = line.strip('\n')
            if len(l) == 0:
                self.netdev.append(details)
                details = {}
            else:
                (k, v) = l.split(':', 1)
                details[k.strip().lower().replace(' ', '-')] = v.strip()
        pipe.wait()

    def as_xml(self):
        output = '<net-devices>\n'

        for d in self.netdev:
            output += ' <net-device'
            for k, v in d.items():
                output += ' %s="%s"' % (k, v)
            output += '/>\n'

        output += '</net-devices>\n'
        return output

### EA-1069

import xcp.logger as LOG
from xcp.pci import VALID_SBDFI
from xcp.net.mac import VALID_COLON_MAC
from xcp.net.ip import ip_link_set_name
from xcp.net.ifrename.logic import rename, VALID_ETH_NAME
from xcp.net.ifrename.macpci import MACPCI
from xcp.net.ifrename.static import StaticRules
from xcp.net.ifrename.dynamic import DynamicRules
from xcp.net.ifrename.util import niceformat

static_rules = StaticRules()
dynamic_rules = DynamicRules()

RX_ETH = VALID_ETH_NAME
RX_MAC = VALID_COLON_MAC
RX_PCI = VALID_SBDFI
RX_PPN = re.compile(r"^(?:em\d+|pci\d+p\d+)$")

def parse_arg(arg):
    """
    Takes list from the code which parses the installer commandline.
    Returns a tupe:
            (Target eth name, Static/Dynamic, Method of id, Val of id)
    or None if the parse was not successful
    """

    split = arg.split(":", 2)

    if len(split) != 3:
        LOG.warning("Invalid device mapping '%s' - Ignoring" % (arg,))
        return

    eth, sd, val = split

    if RX_ETH.match(eth) is None:
        LOG.warning("'%s' is not a valid device name - Ignoring" % (eth,))
        return

    if sd not in ['s', 'd']:
        LOG.warning("'%s' is not valid to distinguish between static/dynamic rules" % (sd,))
        return
    else:
        if sd == 's':
            formulae = static_rules.formulae
        else:
            formulae = dynamic_rules.formulae

    if len(val) < 2:
        LOG.warning("'%s' is not a valid mapping target - Ignoring" % (val,))
        return

    if val[0] == '"' and val[-1] == '"':
        formulae[eth] = ('label', val[1:-1])
    elif RX_MAC.match(val) is not None:
        formulae[eth] = ('mac', val.lower())
    elif RX_PCI.match(val) is not None:
        formulae[eth] = ('pci', val.lower())
    elif RX_PPN.match(val) is not None:
        formulae[eth] = ('ppn', val.lower())
    else:
        LOG.warning("'%s' is not a recognised mapping target - Ignoring" % (val,))


def remap_netdevs(remap_list):

    # # rename everything sideways to safe faffing with temp renanes
    # for x in ( x for x in os.listdir("/sys/class/net/") if x[:3] == "eth" ):
    #     util.runCmd2(['ip', 'link', 'set', x, 'name', 'side-'+x])

    for cmd in remap_list:
        parse_arg(cmd)

        # Grab the current state from biosdevname
    current_eths = all_devices_all_names()
    current_state = []

    for nic in current_eths:
        eth = current_eths[nic]

        if not ( "BIOS device" in eth and
                 "Kernel name" in eth and
                 "Assigned MAC" in eth and
                 "Bus Info" in eth and
                 "all_ethN" in eth["BIOS device"] and
                 "physical" in eth["BIOS device"]
                  ):
            LOG.error("Interface information for '%s' from biosdevname is "
                      "incomplete; Discarding."
                      % (eth.get("Kernel name", "Unknown"),))

        try:
            current_state.append(
                MACPCI(eth["Assigned MAC"],
                       eth["Bus Info"],
                       kname=eth["Kernel name"],
                       order=int(eth["BIOS device"]["all_ethN"][3:]),
                       ppn=eth["BIOS device"]["physical"],
                       label=eth.get("SMBIOS Label", "")
                       ))
        except Exception as e:
            LOG.error("Can't generate current state for interface '%s' - "
                      "%s" % (eth, e))
    current_state.sort()

    LOG.debug("Current state = %s" % (niceformat(current_state),))

    static_rules.generate(current_state)
    dynamic_rules.generate(current_state)

    static_eths = [ x.tname for x in static_rules.rules ]
    last_boot = [ x for x in dynamic_rules.rules if x.tname not in static_eths ]

    LOG.debug("StaticRules Formulae = %s" % (niceformat(static_rules.formulae),))
    LOG.debug("StaticRules Rules = %s" % (niceformat(static_rules.rules),))
    LOG.debug("DynamicRules Lastboot = %s" % (niceformat(last_boot),))

    # Invoke the renaming logic
    try:
        transactions = rename(static_rules=static_rules.rules,
                              cur_state=current_state,
                              last_state=last_boot,
                              old_state=[])
    except Exception as e:
        LOG.critical("Problem from rename logic: %s.  Giving up" % (e,))
        return

    # Apply transactions, or explicitly state that there are none
    if len (transactions):
        for src, dst in transactions:
            ip_link_set_name(src, dst)
    else:
        LOG.info("No transactions.  No need to rename any nics")


    # Regenerate dynamic configuration
    def macpci_as_list(x):
        return [str(x.mac), str(x.pci), x.tname]

    new_lastboot = map(macpci_as_list, current_state)
    dynamic_rules.lastboot = new_lastboot

    LOG.info("All done ordering the network devices")

def disable_ipv6_module(root):
    # Disable IPv6 loading by default.
    # This however does not disable from loading for requiring modules
    # (like bridge)
    dv6fd = open("%s/etc/modprobe.d/disable-ipv6.conf" % root, "w")
    dv6fd.write("alias net-pf-10 off\n")
    dv6fd.close()

