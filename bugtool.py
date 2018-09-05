#!/usr/bin/env python
# Copyright (c) Citrix Systems 2010.  All rights reserved.
# Xen, the Xen logo, XenCenter, XenMotion are trademarks or registered
# trademarks of Citrix Systems, Inc., in the United States and other
# countries.

import os
import os.path
import sys

import xcp.accessor
import xcp.logger

import netutil
import product
import util
import xelogging
from netinterface import *


# Attempt to configure the network:
def configureNetworking(device, config):
    if device == 'all':
        config = 'dhcp'
    config_dict = None
    try:
        if config.startswith('static:'):
            config_dict = {'gateway': None, 'dns': None, 'domain': None}
            for el in config[7:].split(';'):
                k, v = el.split('=', 1)
                config_dict[k] = v
            if 'dns' in config_dict:
                config_dict['dns'] = config_dict['dns'].split(',')
            assert 'ip' in config_dict and 'netmask' in config_dict
    except:
        pass

    nethw = netutil.scanConfiguration()
    netcfg = {}
    for i in nethw.keys():
        if (device == i or device == nethw[i].hwaddr) and config_dict:
            netcfg[i] = NetInterface(NetInterface.Static, nethw[i].hwaddr,
                                     config_dict['ip'], config_dict['netmask'],
                                     config_dict['gateway'], config_dict['dns'],
                                     config_dict['domain'])
        else:
            netcfg[i] = NetInterface(NetInterface.DHCP, nethw[i].hwaddr)

    netutil.writeNetInterfaceFiles(netcfg)
    netutil.writeResolverFile(netcfg, '/etc/resolv.conf')

    if device == 'all':
        for i in nethw.keys():
            netutil.ifup(i)
    elif device.startswith('eth'):
        if nethw.has_key(device):
            netutil.ifup(device)
    else:
        # MAC address
        matching_list = filter(lambda x: x.hwaddr == device, nethw.values())
        if len(matching_list) == 1:
            netutil.ifup(matching_list[0].name)

def bugtool(inst, dest_url):
    try:
        inst.mount_root(ro = False)

        util.bindMount('/dev', os.path.join(inst.root_fs.mount_point, 'dev'))
        util.bindMount('/proc', os.path.join(inst.root_fs.mount_point, 'proc'))
        util.bindMount('/sys', os.path.join(inst.root_fs.mount_point, 'sys'))

        os.environ['XEN_RT'] = '1'
        os.environ['XENRT_BUGTOOL_BASENAME'] = 'offline-bugtool'
        util.runCmd2(['chroot', inst.root_fs.mount_point, '/usr/sbin/xen-bugtool', '-y', '--unlimited'])
        out_fname = os.path.join(inst.root_fs.mount_point, 'var/opt/xen/bug-report/offline-bugtool.tar.bz2')

        util.umount(os.path.join(inst.root_fs.mount_point, 'sys'))
        util.umount(os.path.join(inst.root_fs.mount_point, 'proc'))
        util.umount(os.path.join(inst.root_fs.mount_point, 'dev'))

        xcp.logger.log("Saving to " + dest_url)
        a = xcp.accessor.createAccessor(dest_url, False)
        a.start()
        inh = open(out_fname)
        a.writeFile(inh, 'offline-bugtool.tar.bz2')
        inh.close()
        a.finish()

        os.remove(out_fname)
    finally:
        inst.unmount_root()

def main(args):
    xcp.logger.openLog(sys.stdout)
    xelogging.openLog(sys.stdout)

    dest_url = None
    answer_device = 'all'
    answer_config = 'dhcp'
    init_network = False
    reboot = False
    
    xelogging.log("Command line args: %s" % str(args))

    for (opt, val) in args.items():
        if opt in ['--answerfile_device', '--network_device']:
            answer_device = val.lower()
            init_network = True
        elif opt == '--network_config':
            answer_config = val.lower()
        elif opt == "--reboot":
            reboot = True
        elif opt == "--dest":
            dest_url = val

    if init_network:
        configureNetworking(answer_device, answer_config)

    # probe for XS installations
    insts = product.findXenSourceProducts()
    if len(insts) == 0:
        xcp.logger.log("No installations found.")
        return

    if not dest_url:
        xcp.logger.log("Destination directory not specified.")
        return

    for inst in insts:
        xcp.logger.log(str(inst))
        bugtool(inst, dest_url)

    return reboot


if __name__ == "__main__":
    f = open('/proc/cmdline')
    args = map(lambda x: '--'+x, f.readline().strip().split())
    f.close()
    reboot = main(util.splitArgs(args))
    if reboot:
        os.system("reboot")
