# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this 
# copyrighted material is governed by and subject to terms and conditions 
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or 
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# TUI Network configuration screens
#
# written by Andrew Peace

import uicontroller
from uicontroller import LEFT_BACKWARDS, RIGHT_FORWARDS, REPEAT_STEP
import tui
import tui.progress
import snackutil
import netutil
from netinterface import *
import version
import os

from snack import *

def get_iface_configuration(nic, txt = None, defaults = None, include_dns = False):

    def use_vlan_cb_change():
        vlan_field.setFlags(FLAG_DISABLED, vlan_cb.value())

    def dhcp_change():
        for x in [ ip_field, gateway_field, subnet_field, dns_field ]:
            x.setFlags(FLAG_DISABLED, not dhcp_rb.selected())

    gf = GridFormHelp(tui.screen, 'Networking', 'ifconfig', 1, 8)
    if txt == None:
        txt = "Configuration for %s (%s)" % (nic.name, nic.hwaddr)
    text = TextboxReflowed(45, txt)
    b = [("Ok", "ok"), ("Back", "back")]
    buttons = ButtonBar(tui.screen, b)

    ip_field = Entry(16)
    subnet_field = Entry(16)
    gateway_field = Entry(16)
    dns_field = Entry(16)
    vlan_field = Entry(16)

    if defaults and defaults.isStatic():
        # static configuration defined previously
        dhcp_rb = SingleRadioButton("Automatic configuration (DHCP)", None, 0)
        dhcp_rb.setCallback(dhcp_change, ())
        static_rb = SingleRadioButton("Static configuration:", dhcp_rb, 1)
        static_rb.setCallback(dhcp_change, ())
        if defaults.ipaddr:
            ip_field.set(defaults.ipaddr)
        if defaults.netmask:
            subnet_field.set(defaults.netmask)
        if defaults.gateway:
            gateway_field.set(defaults.gateway)
        if defaults.dns:
            dns_field.set(defaults.dns[0])
    else:
        dhcp_rb = SingleRadioButton("Automatic configuration (DHCP)", None, 1)
        dhcp_rb.setCallback(dhcp_change, ())
        static_rb = SingleRadioButton("Static configuration:", dhcp_rb, 0)
        static_rb.setCallback(dhcp_change, ())
        ip_field.setFlags(FLAG_DISABLED, False)
        subnet_field.setFlags(FLAG_DISABLED, False)
        gateway_field.setFlags(FLAG_DISABLED, False)
        dns_field.setFlags(FLAG_DISABLED, False)

    vlan_cb = Checkbox("Use VLAN:", defaults.isVlan() if defaults else False)
    vlan_cb.setCallback(use_vlan_cb_change, ())
    if defaults and defaults.isVlan():
        vlan_field.set(str(defaults.vlan))
    else:
        vlan_field.setFlags(FLAG_DISABLED, False)

    ip_text = Textbox(15, 1, "IP Address:")
    subnet_text = Textbox(15, 1, "Subnet mask:")
    gateway_text = Textbox(15, 1, "Gateway:")
    dns_text = Textbox(15, 1, "Nameserver:")
    vlan_text = Textbox(15, 1, "VLAN (1-4094):")

    entry_grid = Grid(2, include_dns and 4 or 3)
    entry_grid.setField(ip_text, 0, 0)
    entry_grid.setField(ip_field, 1, 0)
    entry_grid.setField(subnet_text, 0, 1)
    entry_grid.setField(subnet_field, 1, 1)
    entry_grid.setField(gateway_text, 0, 2)
    entry_grid.setField(gateway_field, 1, 2)
    if include_dns:
        entry_grid.setField(dns_text, 0, 3)
        entry_grid.setField(dns_field, 1, 3)

    vlan_grid =  Grid(2, 1)
    vlan_grid.setField(vlan_text, 0, 0)
    vlan_grid.setField(vlan_field, 1, 0)

    gf.add(text, 0, 0, padding = (0, 0, 0, 1))
    gf.add(dhcp_rb, 0, 2, anchorLeft = True)
    gf.add(static_rb, 0, 3, anchorLeft = True)
    gf.add(entry_grid, 0, 4, padding = (0, 0, 0, 1))
    gf.add(vlan_cb, 0, 5, anchorLeft = True)
    gf.add(vlan_grid, 0, 6, padding = (0, 0, 0, 1))
    gf.add(buttons, 0, 7, growx = 1)

    loop = True
    while loop:
        result = gf.run()

        if buttons.buttonPressed(result) in ['ok', None]:
            # validate input
            msg = ''
            if static_rb.selected():
                if not netutil.valid_ip_addr(ip_field.value()):
                    msg = 'IP Address'
                elif not netutil.valid_ip_addr(subnet_field.value()):
                    msg = 'Subnet mask'
                elif gateway_field.value() != '' and not netutil.valid_ip_addr(gateway_field.value()):
                    msg = 'Gateway'
                elif dns_field.value() != '' and not netutil.valid_ip_addr(dns_field.value()):
                    msg = 'Nameserver'
            if vlan_cb.selected():
                if not netutil.valid_vlan(vlan_field.value()):
                    msg = 'VLAN'
            if msg != '':
                tui.progress.OKDialog("Networking", "Invalid %s, please check the field and try again." % msg)
            else:
                loop = False
        else:
            loop = False

    tui.screen.popWindow()

    if buttons.buttonPressed(result) == 'back': return LEFT_BACKWARDS, None

    vlan_value = int(vlan_field.value()) if vlan_cb.selected() else None
    if bool(dhcp_rb.selected()):
        answers = NetInterface(NetInterface.DHCP, nic.hwaddr, vlan=vlan_value)
    else:
        answers = NetInterface(NetInterface.Static, nic.hwaddr, ip_field.value(),
                               subnet_field.value(), gateway_field.value(),
                               dns_field.value(), vlan=vlan_value)
    return RIGHT_FORWARDS, answers

def select_netif(text, conf, offer_existing = False, default = None):
    """ Display a screen that displays a choice of network interfaces to the
    user, with 'text' as the informative text as the data, and conf being the
    netutil.scanConfiguration() output to be used. 
    """

    netifs = conf.keys()
    netifs.sort(lambda l, r: int(l[3:]) - int(r[3:]))

    if default not in netifs:
        # find first link that is up
        default = None
        for iface in netifs:
            if netutil.linkUp(iface):
                default = iface
                break

    def lentry(iface):
        key = iface
        tag = netutil.linkUp(iface) and '          ' or ' [no link]'
        text = "%s (%s)%s" % (iface, conf[iface].hwaddr, tag)
        return (text, key)

    def iface_details(context):
        tui.update_help_line([' ', ' '])
        if context:
            nic = conf[context]

            table = [ ("Name:", nic.name),
                      ("Driver:", nic.driver),
                      ("MAC Address:", nic.hwaddr),
                      ("PCI Details:", nic.pci_string) ]
            if nic.smbioslabel != "":
                table.append(("BIOS Label:", nic.smbioslabel))

            snackutil.TableDialog(tui.screen, "Interface Details", *table)
        else:
            netifs_all = netutil.getNetifList(include_vlan=True)
            details = map(lambda x: (x, netutil.ipaddr(x)), filter(netutil.interfaceUp, netifs_all))
            snackutil.TableDialog(tui.screen, "Networking Details", *details)
        tui.screen.popHelpLine()
        return True

    def update(listbox):
        old = listbox.current()
        for item in listbox.item2key.keys():
            if item:
                text, _ = lentry(item)
                listbox.replace(text, item)
        listbox.setCurrent(old)
        return True

    tui.update_help_line([None, "<F5> more info"])

    def_iface = None
    if offer_existing and netutil.networkingUp():
        netif_list = [("Use existing configuration", None)]
    else:
        netif_list = []
        if default:
            def_iface = lentry(default)
    netif_list += [lentry(x) for x in netifs]
    scroll, height = snackutil.scrollHeight(6, len(netif_list))
    rc, entry = snackutil.ListboxChoiceWindowEx(tui.screen, "Networking", text, netif_list,
                                        ['Ok', 'Back'], 45, scroll, height, def_iface, help = 'selif:info',
                                        hotkeys={'F5': iface_details}, timeout_ms=5000, timeout_cb=update)

    tui.screen.popHelpLine()

    if rc == 'back': return LEFT_BACKWARDS, None
    return RIGHT_FORWARDS, entry

def requireNetworking(answers, defaults=None, msg=None, keys=['net-admin-interface', 'net-admin-configuration']):
    """ Display the correct sequence of screens to get networking
    configuration.  Bring up the network according to this configuration.
    If answers is a dictionary, set 
      answers[keys[0]] to the interface chosen, and 
      answers[keys[1]] to the interface configuration chosen, and
      answers['runtime-iface-configuration'] to current manual network config, in format (all-dhcp, manual-config).
    If defaults.has_key[keys[0]] then use defaults[keys[0]] as the default network interface.
    If defaults.has_key[keys[1]] then use defaults[keys[1]] as the default network interface configuration."""

    interface_key = keys[0]
    config_key = keys[1]

    nethw = answers['network-hardware']
    if len(nethw.keys()) == 0:
        tui.progress.OKDialog("Networking", "No available ethernet device found")
        return REPEAT_STEP

    # Display a screen asking which interface to configure, then what the 
    # configuration for that interface should be:
    def select_interface(answers, default, msg):
        """ Show the dialog for selecting an interface.  Sets
        answers['interface'] to the name of the interface selected (a
        string). """
        if answers.has_key('interface'):
            default = answers['interface']
        if msg == None:
            msg = "%s Setup needs network access to continue.\n\nWhich network interface would you like to configure to access your %s product repository?" % (version.PRODUCT_BRAND or version.PLATFORM_NAME, version.PRODUCT_BRAND or version.PLATFORM_NAME)
        direction, iface = select_netif(msg, nethw, True, default)
        if direction == RIGHT_FORWARDS:
            answers['reuse-networking'] = (iface == None)
            if iface:
                answers['interface'] = iface
        return direction

    def specify_configuration(answers, txt, defaults):
        """ Show the dialog for setting nic config.  Sets answers['config']
        to the configuration used.  Assumes answers['interface'] is a string
        identifying by name the interface to configure. """

        if 'reuse-networking' in answers and answers['reuse-networking']:
            return RIGHT_FORWARDS

        direction, conf = get_iface_configuration(nethw[answers['interface']], txt, 
                                                  defaults=defaults, include_dns=True)
        if direction == RIGHT_FORWARDS:
            answers['config'] = conf
        return direction

    conf_dict = {}
    def_iface = None
    def_conf = None
    if type(defaults) == dict:
        if defaults.has_key(interface_key):
            def_iface = defaults[interface_key]
        if defaults.has_key(config_key):
            def_conf = defaults[config_key]
    if len(nethw.keys()) > 1 or netutil.networkingUp():
        seq = [ uicontroller.Step(select_interface, args=[def_iface, msg]), 
                uicontroller.Step(specify_configuration, args=[None, def_conf]) ]
    else:
        text = "%s Setup needs network access to continue.\n\nHow should networking be configured at this time?" % (version.PRODUCT_BRAND or version.PLATFORM_NAME)
        conf_dict['interface'] = nethw.keys()[0]
        seq = [ uicontroller.Step(specify_configuration, args=[text, def_conf]) ]
    direction = uicontroller.runSequence(seq, conf_dict)

    if direction == RIGHT_FORWARDS and 'config' in conf_dict:
        netutil.writeNetInterfaceFiles(
            {conf_dict['interface']: conf_dict['config']}
            )
        netutil.writeResolverFile(
            {conf_dict['interface']: conf_dict['config']},
            '/etc/resolv.conf'
            )
        tui.progress.showMessageDialog(
            "Networking",
            "Configuring network interface, please wait...",
            )
        ifaceName = conf_dict['config'].getInterfaceName(conf_dict['interface'])
        netutil.ifdown(ifaceName)

        # check that we have *some* network:
        if netutil.ifup(ifaceName) != 0 or not netutil.interfaceUp(ifaceName):
            tui.progress.clearModelessDialog()
            tui.progress.OKDialog("Networking", "The network still does not appear to be active.  Please check your settings, and try again.")
            direction = REPEAT_STEP
        else:
            if answers and type(answers) == dict:
                # write out results
                answers[interface_key] = conf_dict['interface']
                answers[config_key] = conf_dict['config']
                # update cache of manual configurations
                manual_config = {}
                all_dhcp = False
                if answers.has_key('runtime-iface-configuration'):
                    manual_config = answers['runtime-iface-configuration'][1]
                manual_config[conf_dict['interface']] = conf_dict['config']
                answers['runtime-iface-configuration'] = (all_dhcp, manual_config)
            tui.progress.clearModelessDialog()
        
    return direction
