# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this 
# copyrighted material is governed by and subject to terms and conditions 
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or 
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# 'Init' text user interface
#
# written by Andrew Peace

from snack import *
from version import *

import tui
import init_constants
import generalui
import uicontroller
from uicontroller import SKIP_SCREEN, LEFT_BACKWARDS, RIGHT_FORWARDS
import tui.network
import tui.progress
import tui.repo
import repository
import snackutil
import xelogging

def get_keymap():
    entries = generalui.getKeymaps()

    (button, entry) = snackutil.ListboxChoiceWindowEx(
        tui.screen,
        "Select Keymap",
        "Please select the keymap you would like to use:",
        entries,
        ['Ok'], height = 8, scroll = 1, help = "keymap", timeout_ms = 500)

    return entry

def choose_operation(display_restore):
    entries = [ 
        (' * Install or upgrade %s' % BRAND_SERVER, init_constants.OPERATION_INSTALL),
        ]

    if display_restore:
        entries.append( (' * Restore from backup', init_constants.OPERATION_RESTORE) )

    (button, entry) = ListboxChoiceWindow(tui.screen,
                                          "Welcome to %s" % (PRODUCT_BRAND or PLATFORM_NAME),
                                          """Please select an operation:""",
                                          entries,
                                          ['Ok', 'Load driver', 'Exit and reboot'], width=70)

    if button == 'ok' or button == None:
        return entry
    elif button == 'load driver':
        return init_constants.OPERATION_LOAD_DRIVER
    else:
        return init_constants.OPERATION_REBOOT

def driver_disk_sequence(answers, driver_repos):
    uic = uicontroller
    seq = [
        uic.Step(tui.repo.select_repo_source, 
                 args = ["Select Driver Source", "Please select where you would like to load the Supplemental Pack containing the driver from:", 
                         False]),
        uic.Step(tui.network.requireNetworking,
                 predicates = [lambda a: a['source-media'] != 'local']),
        uic.Step(tui.repo.get_source_location, 
                 predicates = [lambda a: a['source-media'] != 'local'],
                 args = [False]),
        uic.Step(tui.repo.confirm_load_repo, args=['driver', driver_repos]),
        ]
    rc = uicontroller.runSequence(seq, answers)

    if rc == LEFT_BACKWARDS:
        return None
    return (answers['source-media'], answers['source-address'])

def select_backup(backups):
    entries = []
    for b in backups:
        backup_partition, restore_disk = b
        entries.append(("%s, to be restored on %s" %
                           (backup_partition[5:], restore_disk[5:]), 
                        b))

    b, e = ListboxChoiceWindow(
        tui.screen,
        'Multiple Backups',
        'More than one backup has been found.  Which would you like to use?',
        entries,
        ['Select', 'Cancel']
        )

    if b in [ None, 'select' ]:
        return e
    else:
        return None

def confirm_restore(backup_partition, disk):
    b = snackutil.ButtonChoiceWindowEx(
        tui.screen,
        "Confirm Restore",
        "Are you sure you want to restore your installation on %s with the backup on %s?\n\nYour existing installation will be overwritten with the backup (though VMs will still be intact).\n\nTHIS OPERATION CANNOT BE UNDONE." % (disk[5:], backup_partition[5:]),
        ['Restore', 'Cancel'], default=1, width=50
        )

    return b in ['restore', None]

def confirm_proceed():
    b = snackutil.ButtonChoiceWindowEx(
        tui.screen,
        "Confirm Local Disk Format",
        "WARNING: proceeding with this installation will reinstall your local hard disk with %s %s" % (PRODUCT_BRAND, PRODUCT_VERSION),
        ['Proceed', 'Cancel'], default=1, width=50
    )

    return b in ['proceed', None]
