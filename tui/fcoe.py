# Copyright (c) 2015 Citrix Systems, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by Citrix Systems, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of Citrix Systems, Inc. in the United States and/or other
# countries.

###
#
# FCoE tui
#
###

import re, sys
import os.path
import constants
import CDROM
import fcntl
import util
import netutil
from util import dev_null
import tui
import xelogging
from disktools import *
import time
import snackutil
from snack import *
import fcoeutil

def select_fcoe_ifaces(answers):
    """ Display a screen that displays all network interfaces that are
    FCoE-capable and allows the user to select one or more.
    """

    conf = netutil.scanConfiguration()
    fcoe_ifaces = fcoeutil.get_dcb_capable_ifaces(True)

    if len(fcoe_ifaces) == 0:
        button = ButtonChoiceWindow(
            tui.screen,
            "FCoE Interfaces",
            "No DCB capable interfaces found",
            ['Back'],
            width=60)

        return

    netifs = fcoe_ifaces.keys()
    netifs.sort(lambda l, r: int(l[3:]) - int(r[3:]))

    def iface_details(context):
        tui.update_help_line([' ', ' '])

        nic = conf[context]

        table = [ ("Name:", nic.name),
                  ("Driver:", nic.driver),
                  ("MAC Address:", nic.hwaddr),
                  ("Link Status:", netutil.linkUp(context) and 'Up' or 'Down') ]

        snackutil.TableDialog(tui.screen, "Interface Details", *table)
        tui.screen.popHelpLine()
        return True

    # Map between soft/off and soft/hard (depending on interface property)
    def dcb_state_label(iface, state):
        if state:
            return '[soft]'
        if not fcoe_ifaces[iface]:
            return '[hard]'
        return '      '

    if 'fcoe-interfaces' not in answers:
        answers['fcoe-interfaces'] = {}

    entries = {}
    for ne in netifs:
        state = dcb_state_label(ne, answers['fcoe-interfaces'].get(ne, fcoe_ifaces[ne]))
        entry = "%s %s" % (ne, state)
        entries[ne] = entry

    text = TextboxReflowed(54, "Select one or more interfaces to setup for FCoE.")
    buttons = ButtonBar(tui.screen, [('Ok', 'ok'), ('DCB', 'dcb'), ('Back', 'back')])
    scroll, _ = snackutil.scrollHeight(3, len(entries.keys()))
    cbt = CheckboxTree(3, scroll)
    for iface in netifs:
        cbt.append(entries[iface], iface, iface in answers['fcoe-interfaces'])

    gf = GridFormHelp(tui.screen, 'FCoE Interfaces', 'fcoeiface:info', 1, 3)
    gf.add(text, 0, 0, padding = (0, 0, 0, 1))
    gf.add(cbt, 0, 1, padding = (0, 0, 0, 1))
    gf.add(buttons, 0, 2, growx = 1)
    gf.addHotKey('F5')

    tui.update_help_line([None, "<F5> more info"])

    loop = True
    while loop:
        rc = gf.run()
        if rc == 'F5':
            iface_details(cbt.getCurrent())
        elif buttons.buttonPressed(rc) == 'dcb':
            ne = cbt.getCurrent()
            new = dcb_state_label(ne, not entries[ne].endswith('[soft]'))
            entries[ne] = "%s %s" % (ne, new)
            cbt.setEntry(ne, entries[ne])
        else:
            loop = False
    tui.screen.popWindow()
    tui.screen.popHelpLine()

    button = buttons.buttonPressed(rc)

    if button == 'back':
        return

    r = dict(map(lambda (k): (k, entries[k].endswith('[soft]')), cbt.getSelection()))
    answers['fcoe-interfaces'] = r
    xelogging.log("Selected fcoe interfaces %s" % str(r))

    tui.update_help_line([' ', ' '])

    # Bring up FCoE devices
    tui.progress.showMessageDialog("Please wait", "Discovering devices...")
    result = fcoeutil.start_fcoe(answers['fcoe-interfaces'])
    xelogging.log("fcoe result %s" % str(result))
    tui.progress.clearModelessDialog()

    fail = {k: v for k, v in result.iteritems() if v != 'OK'}
    if len(fail.keys()) > 0:
        # Report any errors
        err_text = '\n'.join(map(lambda (x, y): "%s %s" % (x, y), fail.iteritems()))
        text = TextboxReflowed(60, "The following errors occured while discovering FCoE disks.")
        errs = Textbox(30, 6, err_text, scroll = len(fail.keys()) > 6)
        buttons = ButtonBar(tui.screen, [('Ok', 'ok')])

        gf = GridFormHelp(tui.screen, 'Discovery Failure', 'fipvlanfail', 1, 3)
        gf.add(text, 0, 0, padding = (0, 0, 0, 1))
        gf.add(errs, 0, 1, padding = (0, 0, 0, 1))
        gf.add(buttons, 0, 2, growx = 1)
        gf.run()
        tui.screen.popWindow()

    # Get the results and build a dict of LUNs
    d = fcoeutil.get_fcoe_luns()

    luns = {}
    for k, v in d.items():
        for k2, v2 in v.items():
            for lun in v2['luns'].values():
                luns[os.path.basename(lun['device'])] = {'Capacity': lun['capacity'], 'Description': lun['description'],
                                                         'Port': v2['Port Name'], 'VLAN': k}

    xelogging.log("fcoe luns discovered %s" % str(luns))
    def disk_details(context):
        tui.update_help_line([' ', ' '])
        table = [("Name:", context)]
        for label in ("VLAN", "Capacity", "Port", "Description"):
            table.append((label+':', luns[context][label]))
        snackutil.TableDialog(tui.screen, "Disk Details", *table)
        tui.screen.popHelpLine()
        return True

    if len(luns.keys()) > 0:
        disk_list = []
        for lun in sorted(luns.keys()):
            disk_list.append(("%s - %s" % (lun, luns[lun]['Capacity']), lun))

        tui.update_help_line([None, "<F5> more info"])
        scroll, height = snackutil.scrollHeight(6, len(disk_list))
        snackutil.ListboxChoiceWindowEx(tui.screen, "FCoE Disks", "The following devices are now available.", disk_list,
                                        ['Ok'], 45, scroll, height, None, help='fcoedisks:info', hotkeys={'F5': disk_details})
        tui.screen.popHelpLine()
