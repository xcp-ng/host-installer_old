# Copyright (c) 2008 Citrix Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by Citrix Inc. All other rights reserved.

###
# XEN HOST INSTALLER
# Wrapper for network interfaces
#
# written by Simon Rowe

import util
import netutil

def getText(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc.strip().encode()
def getTextOrNone(nodelist):
    rc = ""
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc = rc + node.data
    return rc == "" and None or rc.strip().encode()

class NetInterface:
    """ Represents the configuration of a network interface. """

    Static = 1
    DHCP = 2
    Autoconf = 3

    def __init__(self, mode, hwaddr, ipaddr=None, netmask=None, gateway=None,
                 dns=None, domain=None, vlan=None, ipv6=False):
        assert mode in [None, self.Static, self.DHCP, self.Autoconf]
        if ipaddr == '':
            ipaddr = None
        if netmask == '':
            netmask = None
        if gateway == '':
            gateway = None
        if dns == '':
            dns = None
        elif isinstance(dns, str):
            dns = [ dns ]
        if mode == self.Static:
            assert ipaddr
            assert netmask

        self.hwaddr = hwaddr
        if ipv6:
            self.mode = None
            self.ipaddr = None
            self.netmask = None
            self.gateway = None

            self.modev6 = mode
            self.ipv6addr = ipaddr + "/" + netmask if mode == self.Static else None
            self.ipv6_gateway = gateway if mode == self.Static else None
        else:
            self.modev6 = None
            self.ipv6addr = None
            self.ipv6_gateway = None

            self.mode = mode
            self.ipaddr = ipaddr if mode == self.Static else None
            self.netmask = netmask if mode == self.Static else None
            self.gateway = gateway if mode == self.Static else None

        self.dns = dns if mode == self.Static else None
        self.domain = domain if mode == self.Static else None
        self.vlan = vlan

    def __repr__(self):
        hw = "hwaddr = '%s' " % self.hwaddr

        if self.mode == self.Static:
            ipv4 = "Static;" + \
                "ipaddr='%s';netmask='%s';gateway='%s';dns='%s';domain='%s'>" % \
                (self.ipaddr, self.netmask, self.gateway, self.dns, self.domain)
        elif self.mode == self.DHCP:
            ipv4 = "DHCP"
        else:
            ipv4 = "None"

        if self.modev6 == self.Static:
            ipv6 = "Static;" + \
                "ipaddr='%s';gateway='%s'>" % \
                (self.ipv6addr, self.ipv6_gateway)
        elif self.modev6 == self.DHCP:
            ipv6 = "DHCP"
        elif self.modev6 == self.Autoconf:
            ipv6 = "autoconf"
        else:
            ipv6 = "None"
        vlan = ("vlan = '%d' " % self.vlan) if self.vlan else ""

        return "<NetInterface: %s%s ipv4:%s ipv6:%s>" % (hw, vlan, ipv4, ipv6)

    def get(self, name, default=None):
        retval = default
        if hasattr(self, name):
            attr = getattr(self, name)
            if attr is not None:
                retval = attr
        return retval

    def getInterfaceName(self, iface):
        return ("%s.%d" % (iface, self.vlan)) if self.vlan else iface

    def addIPv6(self, modev6, ipv6addr=None, ipv6gw=None):
        assert modev6 is None or modev6 == self.Static or modev6 == self.DHCP or modev6 == self.Autoconf
        if ipv6addr == '':
            ipv6addr = None
        if ipv6gw == '':
            ipv6gw = None
        if modev6 == self.Static:
            assert ipv6addr

        self.modev6 = modev6
        if modev6 == self.Static:
            self.ipv6addr = ipv6addr
            self.ipv6_gateway = ipv6gw
        else:
            self.ipv6addr = None
            self.ipv6_gateway = None

    def valid(self):
        if (self.mode == self.Static) and ((self.ipaddr is None) or (self.netmask is None)):
            return False
        if (self.modev6 == self.Static) and (self.ipv6addr is None):
            return False
        return self.mode or self.modev6

    def isStatic(self):
        """ Returns true if a static interface configuration is represented. """
        return self.mode == self.Static or (self.mode == None and self.modev6 == self.Static)

    def isDHCP(self):
        return self.mode == self.DHCP or (self.mode == None and self.modev6 == self.DHCP)

    def isVlan(self):
        return self.vlan is not None

    def getBroadcast(self):
        bcast = None
        rc, output = util.runCmd2(['/bin/ipcalc', '-b', self.ipaddr, self.netmask],
                                  with_stdout=True)
        if rc == 0:
            bcast = output[10:].strip()
        return bcast

    def writeDebStyleInterface(self, iface, f):
        """ Write a Debian-style configuration entry for this interface to
        file object f using interface name iface. """

        # Debian style interfaces are only used for the installer; dom0 only uses CentOS style
        # IPv6 is only enabled through answerfiles and so is not supported here.
        assert self.modev6 or self.mode
        iface_vlan = self.getInterfaceName(iface)

        if self.mode == self.DHCP:
            f.write("iface %s inet dhcp\n" % iface_vlan)
        elif self.mode == self.Static:
            # CA-11825: broadcast needs to be determined for non-standard networks
            bcast = self.getBroadcast()
            f.write("iface %s inet static\n" % iface_vlan)
            f.write("   address %s\n" % self.ipaddr)
            if bcast is not None:
                f.write("   broadcast %s\n" % bcast)
            f.write("   netmask %s\n" % self.netmask)
            if self.gateway:
                f.write("   gateway %s\n" % self.gateway)

        if self.modev6 == self.DHCP:
            f.write("iface %s inet6 dhcp\n" % iface_vlan)
        if self.modev6 == self.Autoconf:
            f.write("iface %s inet6 auto\n" % iface_vlan)
        elif self.modev6 == self.Static:
            f.write("iface %s inet6 static\n" % iface_vlan)
            f.write("   address %s\n" % self.ipv6addr)
            if self.ipv6_gateway:
                f.write("   gateway %s\n" % self.ipv6_gateway)

    def writeRHStyleInterface(self, iface):
        """ Write a RedHat-style configuration entry for this interface to
        file object f using interface name iface. """

        assert self.modev6 or self.mode
        iface_vlan = self.getInterfaceName(iface)

        f = open('/etc/sysconfig/network-scripts/ifcfg-%s' % iface_vlan, 'w')
        f.write("DEVICE=%s\n" % iface_vlan)
        f.write("ONBOOT=yes\n")
        if self.mode == self.DHCP or self.modev6 == self.DHCP:
            f.write("BOOTPROTO=dhcp\n")
            f.write("PERSISTENT_DHCLIENT=1\n")
        else:
            f.write("BOOTPROTO=none\n")

        if self.mode == self.Static:
            # CA-11825: broadcast needs to be determined for non-standard networks
            bcast = self.getBroadcast()
            f.write("IPADDR=%s\n" % self.ipaddr)
            if bcast is not None:
                f.write("BROADCAST=%s\n" % bcast)
            f.write("NETMASK=%s\n" % self.netmask)
            if self.gateway:
                f.write("GATEWAY=%s\n" % self.gateway)

        if self.modev6:
            f.write("NETWORKING_IPV6=yes\n")
            f.write("IPV6INIT=yes\n")
            f.write("IPV6_AUTOCONF=yes\n" if self.modev6 == self.Autoconf else "IPV6_AUTOCONF=no\n")
        if self.modev6 == self.DHCP:
            f.write("DHCPV6C=yes\n")
        elif self.modev6 == self.Static:
            f.write("IPV6ADDR=%s\n" % self.ipv6addr)
            if self.ipv6_gateway:
                prefix = self.ipv6addr.split("/")[1]
                f.write("IPV6_DEFAULTGW=%s/%s\n" % (self.ipv6_gateway, prefix))

        if self.vlan:
            f.write("VLAN=yes\n")

        f.close()


    def waitUntilUp(self, iface):
        if not self.isStatic():
            return True
        if not self.gateway:
            return True

        rc = util.runCmd2(['/usr/sbin/arping', '-f', '-w', '120', '-I',
                           self.getInterfaceName(iface), self.gateway])
        return rc == 0

    @staticmethod
    def getModeStr(mode):
        if mode == NetInterface.Static:
            return 'static'
        if mode == NetInterface.DHCP:
            return 'dhcp'
        if mode == NetInterface.Autoconf:
            return 'autoconf'
        return 'none'

    @staticmethod
    def loadFromIfcfg(filename):
        def valOrNone(d, k):
            return k in d and d[k] or None

        conf = util.readKeyValueFile(filename)
        mode = None
        if 'BOOTPROTO' in conf:
            if conf['BOOTPROTO'] == 'static' or 'IPADDR' in conf:
                mode = NetInterface.Static
            elif conf['BOOTPROTO'] == 'dhcp':
                mode = NetInterface.DHCP

        hwaddr = valOrNone(conf, 'HWADDR')
        if not hwaddr:
            hwaddr = valOrNone(conf, 'MACADDR')
        if not hwaddr:
            hwaddr = netutil.getHWAddr(conf['DEVICE'])
        dns = None
        n = 1
        while 'DNS%d' % n in conf:
            if not dns: dns = []
            dns.append(conf['DNS%d' % n])
            n += 1

        modev6 = None
        if 'DHCPV6C' in conf:
            modev6 = NetInterface.DHCP
        elif 'IPV6_AUTOCONF' in conf:
            modev6 = NetInterface.Autoconf
        elif 'IPV6INIT' in conf:
            modev6 = NetInterface.Static

        ni = NetInterface(mode, hwaddr, valOrNone(conf, 'IPADDR'), valOrNone(conf, 'NETMASK'),
                            valOrNone(conf, 'GATEWAY'), dns, valOrNone(conf, 'DOMAIN'))
        ni.addIPv6(modev6, valOrNone(conf, 'IPV6ADDR'), valOrNone(conf, 'IPV6_DEFAULTGW'))
        return ni

    @staticmethod
    def loadFromPif(pif):
        mode_txt = getText(pif.getElementsByTagName('ip_configuration_mode')[0].childNodes)
        mode = None
        if mode_txt == 'Static':
            mode = NetInterface.Static
        elif mode_txt == 'DHCP':
            mode = NetInterface.DHCP

        hwaddr = getTextOrNone(pif.getElementsByTagName('MAC')[0].childNodes)
        ipaddr = None
        netmask = None
        gateway = None
        dns = None
        domain = None

        if mode == NetInterface.Static:
            ipaddr = getTextOrNone(pif.getElementsByTagName('IP')[0].childNodes)
            netmask = getTextOrNone(pif.getElementsByTagName('netmask')[0].childNodes)
            gateway = getTextOrNone(pif.getElementsByTagName('gateway')[0].childNodes)
            dns_txt = getText(pif.getElementsByTagName('DNS')[0].childNodes)
            if dns_txt != '':
                dns = dns_txt.split(',')
            domain_list = pif.getElementsByTagName('other_config')[0].getElementsByTagName('domain')
            if len(domain_list) == 1:
                domain = getText(domain_list[0].childNodes)

        mode_txt = ''
        modev6 = None
        ipv6addr = None
        gatewayv6 = None
        try:
            mode_txt = getText(pif.getElementsByTagName('ipv6_configuration_mode')[0].childNodes)
        except:
            pass

        if mode_txt == 'Static':
            modev6 = NetInterface.Static
        elif mode_txt == 'DHCP':
            modev6 = NetInterface.DHCP
        elif mode_txt == 'Autoconf':
            modev6 = NetInterface.Autoconf
        if modev6 == NetInterface.Static:
            ipv6addr = getTextOrNone(pif.getElementsByTagName('IPv6')[0].childNodes)
            try:
                gatewayv6 = getTextOrNone(pif.getElementsByTagName('IPv6_gateway')[0].childNodes)
            except:
                gatewayv6 = None

        nic = NetInterface(mode, hwaddr, ipaddr, netmask, gateway, dns, domain)
        nic.addIPv6(modev6, ipv6addr, gatewayv6)
        return nic

    @staticmethod
    def loadFromNetDb(jdata, hwaddr):
        mode = None
        ipaddr = None
        netmask = None
        gateway = None
        dns = None
        domain = None

        try:
            if isinstance(jdata['ipv4_conf'], list):
                if jdata['ipv4_conf'][0] == 'DHCP4':
                    mode = NetInterface.DHCP
                elif jdata['ipv4_conf'][0] == 'Static4':
                    ipaddr = jdata['ipv4_conf'][1][0][0].encode()
                    netmask = netutil.prefix2netmask(jdata['ipv4_conf'][1][0][1])
                    if 'ipv4_gateway' in jdata:
                        gateway = jdata['ipv4_gateway'].encode()
                    if 'dns' in jdata:
                        if len(jdata['dns'][0]) > 0:
                            dns = map(lambda x: x.encode(), jdata['dns'][0])
                        if len(jdata['dns'][1]) > 0:
                            domain = jdata['dns'][1][0].encode()
                    mode = NetInterface.Static
        except:
            pass

        nic = NetInterface(mode, hwaddr, ipaddr, netmask, gateway, dns, domain)

        modev6 = None
        ipv6addr = None
        gatewayv6 = None

        try:
            if isinstance(jdata['ipv6_conf'], list):
                if jdata['ipv6_conf'][0] == 'DHCP6':
                    modev6 = NetInterface.DHCP
                elif jdata['ipv6_conf'][0] == 'Autoconf6':
                    modev6 = NetInterface.Autoconf
                elif jdata['ipv6_conf'][0] == 'Static6':
                    ipv6addr = jdata['ipv6_conf'][1][0].encode()
                    gatewayv6 = jdata['ipv6_gateway'].encode()
                    modev6 = NetInterface.Static
        except:
            pass

        nic.addIPv6(modev6, ipv6addr, gatewayv6)
        return nic
