# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Text user interface progress and message functions
#
# written by Andrew Peace

from snack import *
import tui

PLEASE_WAIT_STRING = "  Working: Please wait..."

def initProgressDialog(title, text, total):
    form = GridFormHelp(tui.screen, title, None, 1, 3)

    t = Textbox(60, 1, text)
    scale = Scale(60, total)
    form.add(t, 0, 0, padding=(0, 0, 0, 1))
    form.add(scale, 0, 1, padding=(0, 0, 0, 0))

    form.draw()
    tui.screen.pushHelpLine(PLEASE_WAIT_STRING)
    tui.screen.refresh()

    return (form, t, scale)

def showMessageDialog(title, text):
    form = GridFormHelp(tui.screen, title, None, 1, 1)

    t = TextboxReflowed(60, text)
    form.add(t, 0, 0, padding=(0, 0, 0, 0))

    form.draw()

    tui.screen.pushHelpLine(PLEASE_WAIT_STRING)
    tui.screen.refresh()

def displayProgressDialog(current, (form, t, scale), updated_text=None):
    scale.set(current)
    if updated_text:
        t.setText(updated_text)

    form.draw()
    tui.screen.refresh()

def clearModelessDialog():
    tui.screen.popHelpLine()
    tui.screen.popWindow()

def OKDialog(title, text, hasCancel=False):
    buttons = ['Ok']
    if hasCancel:
        buttons.append('Cancel')
    return ButtonChoiceWindow(tui.screen, title, text, buttons)
