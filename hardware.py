#!/usr/bin/env python
# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Hardware discovery tools
#
# written by Andrew Peace

import constants
import util
import re
import os.path
from xcp import logger

import xen.lowlevel.xc as xc
XC = xc.xc()
PHYSINFO = XC.physinfo()
XENINFO = XC.xeninfo()

################################################################################
# Functions to get characteristics of the host.  Can work in a VM too, to aid
# with developer testing.

def VTSupportEnabled():
    """ Checks if VT support is present.  Uses /sys/hypervisor to do so,
    expecting a single line in this file with a space separated list of
    capabilities. """
    f = open(constants.HYPERVISOR_CAPS_FILE, 'r')
    caps = ""
    try:
        caps = f.readline()
    finally:
        f.close()

    return "hvm-3.0-x86_32" in caps.strip().split(" ")

def VM_getHostTotalMemoryKB():
    # Use /proc/meminfo to get this.  It has a MemFree entry with the value we
    # need.  The format is lines like this "XYZ:     123 kB".
    meminfo = {}

    f = open("/proc/meminfo", "r")
    try:
        for line in f:
            k, v = line.split(":")
            meminfo[k.strip()] = int(v.strip()[:-3])
    finally:
        f.close()

    return meminfo['MemTotal']

def PhysHost_getHostTotalMemoryKB():

    if PHYSINFO is None or 'total_memory' not in PHYSINFO:
        raise RuntimeError("Unable to determine host memory")

    return PHYSINFO['total_memory']

def VM_getSerialConfig():
    return None

def PhysHost_getSerialConfig():

    if XENINFO is None or 'xen_commandline' not in XENINFO:
        return None

    m = re.match(r'.*(com\d=\S+)', XENINFO['xen_commandline'])
    return m and m.group(1) or None

def PhysHost_getHostTotalCPUs():

    if PHYSINFO is None or 'nr_cpus' not in PHYSINFO:
        raise RuntimeError("Unable to determine number of CPUs")

    return PHYSINFO['nr_cpus']

getHostTotalMemoryKB = PhysHost_getHostTotalMemoryKB
getSerialConfig = PhysHost_getSerialConfig
getHostTotalCPUs = PhysHost_getHostTotalCPUs

def useVMHardwareFunctions():
    global getHostTotalMemoryKB, getSerialConfig
    getHostTotalMemoryKB = VM_getHostTotalMemoryKB
    getSerialConfig = VM_getSerialConfig

def is_serialConsole(console):
    return console.startswith('hvc') or console.startswith('ttyS')

class SerialPort:
    def __init__(self, idv, dev=None, port=None, baud='9600', data='8',
                 parity='n', stop='1', term='vt102', extra=''):
        if not dev:
            dev = "hvc0"
        if not port:
            port = "com%d" % (idv+1)

        self.id = idv
        self.dev = dev
        self.port = port
        self.baud = baud
        self.data = data
        self.parity = parity
        self.stop = stop
        self.term = term
        self.extra = extra

    @classmethod
    def from_string(cls, console):
        """Create instance from Xen console parameter (e.g. com1=115200,8n1)"""
        port = 'com1'
        baud = '9600'
        data = '8'
        parity = 'n'
        stop = '1'
        extra = ''

        m = re.match(r'(com\d+)=(\d+)(?:/\d+)?(?:,(\d)(.)?(\d)?)?((?:,.*)*)$', console)
        if m:
            port = m.group(1)
            baud = m.group(2)
            if m.group(3):
                data = m.group(3)
            if m.group(4):
                parity = m.group(4)
            if m.group(5):
                stop = m.group(5)
            if m.group(6):
                extra = m.group(6)

        return cls(0, None, port, baud, data, parity, stop, extra=extra)

    def __repr__(self):
        return "<SerialPort: %s>" % self.xenFmt()

    def kernelFmt(self):
        return self.dev

    def xenFmt(self):
        return "%s=%s,%s%s%s%s" % (self.port, self.baud, self.data,
                                   self.parity, self.stop, self.extra)
