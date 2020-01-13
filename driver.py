#!/usr/bin/env python
# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Main script
#
# written by Andrew Peace

import sys

# user-interface stuff:
from snack import *
import tui.installer
import tui.installer.screens
import tui.progress
import util

# backend
import repository

# general
from version import *
from xcp import logger

def doInteractiveLoadDriver(ui, answers):
    media = None
    address = None
    required_repo_list = []
    loaded_drivers = []

    rc = ui.init.driver_disk_sequence(answers, answers['driver-repos'])
    if rc:
        media, address = rc
        repos = answers['repos']

        # now load the drivers:
        for r in repos:
            logger.log("Processing repo %s" % r)
            try:
                r.installPackages(lambda x: (), {'root': '/'})
                answers['driver-repos'].append(str(r))

                ButtonChoiceWindow(
                    ui.screen,
                    "Drivers Loaded",
                    "Loaded %s." % r.name(),
                    ['Ok'])
            except Exception as e:
                logger.logException(e)
                ButtonChoiceWindow(
                    ui.screen,
                    "Problem Loading Driver",
                    "Setup was unable to load the device driver.",
                    ['Ok']
                    )

    return media, address

def main(args):
    if len(doInteractiveLoadDriver(tui, {})) > 0:
        return 0
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main(util.splitArgs(sys.argv[1:])))
