# Copyright (c) 2015 Citrix Systems, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by Citrix Systems, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of Citrix Systems, Inc. in the United States and/or other
# countries.

###
#
# FCoE util
#
###

import re, sys
import os.path
import constants
import util
import netutil
from util import dev_null
from xcp import logger
from disktools import *
import time

def start_lldpad():
    util.runCmd2(['/sbin/lldpad', '-d'])

    # Wait for lldpad to be ready
    retries = 0
    while True:
        retries += 1
        if util.runCmd2(['lldptool', '-p']) == 0:
            break
        if retries == 10:
            raise Exception('Timed out waiting for lldpad to be ready')
        time.sleep(1)

def hw_lldp_capable(intf):
    return netutil.getDriver(intf) == 'bnx2x'

def start_fcoe(interfaces):
    ''' startFCoE takes a list of interfaces

        and returns a dictonary {interface:result}
        result could be either OK or error returned from fipvlan
    '''

    '''
    modprobe sg (scsi generic)
    modprobe bnx2fc if required.
    '''

    dcb_wait = True
    result = {}

    start_lldpad()
    util.runCmd2(['/sbin/modprobe', 'sg'])
    util.runCmd2(['/sbin/modprobe', 'libfc'])
    util.runCmd2(['/sbin/modprobe', 'fcoe'])
    util.runCmd2(['/sbin/modprobe', 'bnx2fc'])

    for interface in interfaces:
        if hw_lldp_capable(interface):
            if dcb_wait:
                # Wait for hardware to do dcb negotiation
                dcb_wait = False
                time.sleep(15)

            util.runCmd2(['/sbin/lldptool', '-i', interface, '-L',
                          'adminStatus=disabled'])
        else:
            # Ideally this would use fcoemon to start FCoE but this doesn't
            # fit the host-installer use case because it is possible to start
            # one interface at a time.
            util.runCmd2(['/sbin/dcbtool', 'sc', interface, 'dcb', 'on'])
            util.runCmd2(['/sbin/dcbtool', 'sc', interface, 'app:fcoe',
                          'e:1'])
            util.runCmd2(['/sbin/dcbtool', 'sc', interface, 'pfc',
                          'e:1', 'a:1', 'w:1'])

            # wait for dcbtool changes to take effect
            time.sleep(1)

        logger.log('Starting fipvlan on %s' % interface)

        rc, err = util.runCmd2(['/usr/sbin/fipvlan', '-s', '-c', interface],
                                with_stderr=True)
        if rc != 0:
            result[interface] = err
        else:
            result[interface] = 'OK'

    logger.log(result)

    # Wait for block devices to appear.
    # Without being able to know how long this will take and because LUNs can
    # appear before the block devices are created, just wait a constant number
    # of seconds for FCoE to stabilize.
    time.sleep(30)
    util.runCmd2(util.udevsettleCmd())
    for interface, status in result.iteritems():
        if status == 'OK':
            logger.log(get_luns_on_intf(interface))

    return result

def get_fcoe_capable_ifaces(check_lun):
    ''' Return all FCoE capable interfaces.
        if checkLun is True, then this routine
        will check if there are any LUNs associated
        with an interface and will exclued them
        from the list that is returned.
    '''

    start_lldpad()

    dcb_nics = []
    nics = netutil.scanConfiguration()

    def get_dcb_capablity(interface):
        ''' checks if a NIC is dcb capable (in hardware or software).
            If netdev for an interface has dcbnl_ops defined
            then this interface is deemed dcb capable.
            dcbtool gc ethX dcb will return Status = Successful if netdev
            has dcbnl_ops defined.
        '''

        output = None
        rc, output, err = util.runCmd2(['dcbtool', 'gc', interface, 'dcb'],
                                        True, True)
        if rc != 0:
            return False
        if output is not None:
            outlist = output.split('\n')
            outstr = outlist[3]
            outdata = outstr.split(':')
            return "Successful" in outdata[1]

    for nic, conf in nics.iteritems():
        if get_dcb_capablity(nic):
            if check_lun and len(get_luns_on_intf(nic)) > 0:
                continue
            dcb_nics.append(nic)

    return dcb_nics

def get_fcoe_vlans(interface):
    ''' This routine return fcoe vlans associated with an interface.
        returns the vlans as a list.
    '''

    vlans = []
    rc, out, err = util.runCmd2(['fcoeadm', '-f'], True, True)
    if rc != 0:
        return vlans

    for l in out.split('\n'):
        line = l.strip()

        if len(line) == 0 or ':' not in line:
            continue

        k, v = line.split(':', 1)
        key = k.strip()
        value = v.strip()

        if key == 'Interface':
            iface = value.split('.', 1)[0].strip()

            if iface == interface:
                vlans.append(value)
    return vlans

lun_re = re.compile(r'(\d+)\s+(\S+)\s+(\S+ \S+)\s+(\d+)\s+(.+)')

def get_fcoe_luns():
    ''' returns a dictionary of fcoe luns
    '''

    d = {}
    rc, out, err = util.runCmd2(['fcoeadm', '-t'], True, True)
    if rc != 0:
        return d

    state = 'header'

    for l in out.split('\n'):
        line = l.lstrip()
        if len(line) == 0 or line.startswith('--') or line.startswith('PCI') or line.startswith('No'):
            continue

        if state == 'header':
            if ':' not in line:
                # LUN banner
                rport = header['OS Device Name']
                if iface not in d:
                    d[iface] = {}
                d[iface][rport] = header
                state = 'luns'
                continue

            k, v = line.split(':', 1)
            key = k.strip()
            value = v.strip()

            if key == 'Interface':
                iface = value
                header = {}
            else:
                header[key] = value
        else:
            # LUNs
            m = lun_re.match(line)
            if m:
                if 'luns' not in d[iface][rport]:
                    d[iface][rport]['luns'] = {}
                d[iface][rport]['luns'][m.group(1)] = {'device': m.group(2), 'capacity': m.group(3),
                                                       'bsize': m.group(4), 'description': m.group(5)}
            else:
                if not line.startswith('Interface:'):
                    # Skip LUNs which do not yet have a block device.
                    continue
                # New header, starts with Interface:
                state = 'header'
                _, v = line.split(':', 1)
                iface = v.strip()
                header = {}

    return d



def get_luns_on_intf(interface):
    ''' this routine get all the luns/block devices
        available through interface and returns them
        as a list.
    '''

    fcoedisks = get_fcoe_luns()
    vlans = get_fcoe_vlans(interface)

    lluns = []

    for vlan in vlans:
        if vlan in fcoedisks:
            for rport, val in fcoedisks[vlan].iteritems():
                for lun in val['luns'].values():
                    lluns.append(lun['device'])

    return lluns
