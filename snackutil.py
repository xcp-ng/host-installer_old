# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this 
# copyrighted material is governed by and subject to terms and conditions 
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or 
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Text user interface helper functions
#
# written by Andrew Peace

from snack import *

def ListboxChoiceWindowEx(screen, title, text, items, 
            buttons = ('Ok', 'Cancel'), 
            width = 40, scroll = 0, height = -1, default = None,
            help = None, hotkeys = {},
            timeout_ms = 0, timeout_cb = None):
    if (height == -1): height = len(items)

    bb = ButtonBar(screen, buttons)
    t = TextboxReflowed(width, text)
    l = Listbox(height, scroll = scroll, returnExit = 1)
    count = 0
    for item in items:
        if (type(item) == types.TupleType):
            (text, key) = item
        else:
            text = item
            key = count

        if (default == count):
            default = key
        elif (default == item):
            default = key

        l.append(text, key)
        count = count + 1

    if (default != None):
        l.setCurrent(default)

    g = GridFormHelp(screen, title, help, 1, 3)
    g.add(t, 0, 0)
    g.add(l, 0, 1, padding = (0, 1, 0, 1))
    g.add(bb, 0, 2, growx = 1)
    for k in hotkeys.keys():
        g.addHotKey(k)
    if timeout_ms > 0:
        g.setTimer(timeout_ms)

    loop = True
    while loop:
        rc = g.run()
        if rc == 'TIMER':
            if timeout_cb:
                loop = timeout_cb(l)
        elif rc in hotkeys:
            loop = hotkeys[rc](l.current())
        else:
            loop = False
    screen.popWindow()
    
    return (bb.buttonPressed(rc), l.current())

def ButtonChoiceWindowEx(screen, title, text, 
               buttons = [ 'Ok', 'Cancel' ], 
               width = 40, x = None, y = None, help = None,
               default = 0, hotkeys = {},
               timeout_ms = 0, timeout_cb = None):
    bb = ButtonBar(screen, buttons)
    t = TextboxReflowed(width, text, maxHeight = screen.height - 12)

    g = GridFormHelp(screen, title, help, 1, 2)
    g.add(t, 0, 0, padding = (0, 0, 0, 1))
    g.add(bb, 0, 1, growx = 1)

    g.draw()                                                                   
    g.setCurrent(bb.list[default][0])
    
    for k in hotkeys.keys():
        g.addHotKey(k)
    if timeout_ms > 0:
        g.setTimer(timeout_ms)

    loop = True
    while loop:
        rc = g.run(x, y)
        if rc == 'TIMER':
            if timeout_cb:
                loop = timeout_cb()
        elif rc in hotkeys:
            loop = hotkeys[rc]()
        else:
            loop = False
    screen.popWindow()

    return bb.buttonPressed(rc)

def PasswordEntryWindow(screen, title, text, prompts, allowCancel = 1, width = 40,
                        entryWidth = 20, buttons = [ 'Ok', 'Cancel' ], help = None):
    bb = ButtonBar(screen, buttons)
    t = TextboxReflowed(width, text)

    count = 0
    for n in prompts:
        count = count + 1

    sg = Grid(2, count)

    count = 0
    entryList = []
    for n in prompts:
        if (type(n) == types.TupleType):
            (n, e) = n
        else:
            e = Entry(entryWidth, password = 1)

        sg.setField(Label(n), 0, count, padding = (0, 0, 1, 0), anchorLeft = 1)
        sg.setField(e, 1, count, anchorLeft = 1)
        count = count + 1
        entryList.append(e)

    g = GridFormHelp(screen, title, help, 1, 3)

    g.add(t, 0, 0, padding = (0, 0, 0, 1)) 
    g.add(sg, 0, 1, padding = (0, 0, 0, 1))
    g.add(bb, 0, 2, growx = 1)

    result = g.runOnce()

    entryValues = []
    count = 0
    for n in prompts:
        entryValues.append(entryList[count].value())
        count = count + 1

    return (bb.buttonPressed(result), tuple(entryValues))

def OKDialog(screen, title, text, hasCancel = False, width = 40):
    if hasCancel:
        buttons = ['Ok', 'Cancel']
    else:
        buttons = ['Ok']
    return ButtonChoiceWindow(screen, title, text, buttons, width)

PLEASE_WAIT_STRING = "  Working: Please wait..."

def initProgressDialog(screen, title, text, total):
    form = GridFormHelp(screen, title, None, 1, 3)
    
    t = Textbox(60, 1, text)
    scale = Scale(60, total)
    form.add(t, 0, 0, padding = (0, 0, 0, 1))
    form.add(scale, 0, 1, padding = (0, 0, 0, 0))

    form.draw()
    screen.pushHelpLine(PLEASE_WAIT_STRING)
    screen.refresh()

    return (form, t, scale)

def showMessageDialog(screen, title, text):
    form = GridFormHelp(screen, title, None, 1, 1)
    
    t = TextboxReflowed(60, text)
    form.add(t, 0, 0, padding = (0, 0, 0, 0))

    form.draw()

    screen.pushHelpLine(PLEASE_WAIT_STRING)
    screen.refresh()

def displayProgressDialog(screen, current, (form, t, scale), updated_text = None):
    scale.set(current)
    if updated_text:
        t.setText(updated_text)

    form.draw()
    screen.refresh()

def clearModelessDialog(screen):
    screen.popHelpLine()
    screen.popWindow()

def TableDialog(screen, title, *table):
    wrap_value = 35

    gf = GridFormHelp(screen, title, None, 1, 2)
    bb = ButtonBar(screen, [ 'Ok' ])

    max_label = 0
    max_value = 0
    for label, value in table:
        if len(label) > max_label:
            max_label = len(label)
        if len(value) > max_value:
            max_value = len(value)
    if max_label > 15:
        max_label = 15
    if max_value > wrap_value:
        max_value = wrap_value

    grid = Grid(2, len(table))
    row = 0
    for label, value in table:
        grid.setField(Textbox(max_label+1, 1, label), 0, row, anchorLeft = 1, anchorTop = 1)
        if len(value) > wrap_value:
            tb = TextboxReflowed(wrap_value, value)
        else:
            tb = Textbox(max_value+1, 1, value)
        grid.setField(tb, 1, row, anchorLeft = 1)
        row += 1
    
    gf.add(grid, 0, 0, padding = (0, 0, 0, 1))
    gf.add(bb, 0, 1, growx = 1)

    gf.runOnce()

def scrollHeight(max_height, list_len):
    """ Return height & scroll parameters such that:
    if list_len >= max_height: scroll else: don't scroll """
    if list_len < max_height:
        return 0, -1
    else:
        return 1, max_height
