# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Text user interface functions
#
# written by Andrew Peace

from snack import *
from version import *
import snackutil
import pdb
import traceback
import constants
import sys
from xcp import logger

screen = None
help_pad = [33, 17, 16]
help_line = ["<Tab>/<Alt-Tab> between elements", "", "<F1> Help screen"]


def global_help(screen, context):
    text = """To navigate between fields and buttons use <Tab> to move forwards and <Alt-Tab> to move backwards.

To select check boxes and radio buttons press <Space>.

To advance to the next screen navigate to the Ok button and press Enter or press <F12>."""
    if 'info' in context:
        text += "\n\nTo view additional details about a highlighted item press <F5>."

    OKDialog("General Help", text, width=50)

def init_ui():
    global screen
    screen = SnackScreen()
    screen.drawRootText(0, 0, "Welcome to %s - Version %s" % (PRODUCT_BRAND or PLATFORM_NAME, PRODUCT_VERSION or PLATFORM_VERSION))
    if PRODUCT_BRAND:
        if len(COPYRIGHT_YEARS) > 0:
            screen.drawRootText(0, 1, "Copyright (c) %s %s" % (COPYRIGHT_YEARS, COMPANY_NAME_LEGAL))
    update_help_line(help_line)
    screen.helpCallback(global_help)

def end_ui():
    global screen
    if screen:
        screen.finish()

def update_help_line(help):
    hl = []
    for i in range(0, len(help_line)):
        if len(help) > i and help[i]:
            hl.append(help[i].ljust(help_pad[i]))
        else:
            hl.append(help_line[i].ljust(help_pad[i]))

    screen.pushHelpLine('  ' + '  |  '.join(hl))

def OKDialog(title, text, hasCancel=False, width=40):
    return snackutil.OKDialog(screen, title, text, hasCancel, width)

def exn_error_dialog(logname, with_hd, interactive=True):
    if screen:
        _, exn, _ = sys.exc_info()
        exn_str = str(exn)

        text = constants.error_string(exn_str, logname, with_hd)

        bb = ButtonBar(screen, ['Reboot'])
        t = TextboxReflowed(50, text, maxHeight=screen.height - 13)
        screen.pushHelpLine("  Press <Enter> to reboot.")
        g = GridFormHelp(screen, "Error occurred", None, 1, 2)
        g.add(t, 0, 0, padding=(0, 0, 0, 1))
        g.add(bb, 0, 1, growx=1)
        g.addHotKey("F2")
        if not interactive:
            g.setTimer(constants.AUTO_EXIT_TIMER)
        result = g.runOnce()
        screen.popHelpLine()

        # did they press the secret F2 key that activates debugging
        # features?
        if result == "F2":
            traceback_dialog()
    else:
        logger.log("A text UI error dialog was requested, but the UI has not been initialized yet.")

def traceback_dialog():
    exn_type, exn, tb = sys.exc_info()
    result = ButtonChoiceWindow(
        screen, "Traceback",
        "The traceback was as follows:\n\n" + str.join("", traceback.format_exception(exn_type, exn, tb)),
        ['Ok', 'Start PDB'], width=60
        )
    if result == "start pdb":
        screen.suspend()
        pdb.post_mortem(tb)
        screen.resume()
    del tb

