# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Constants for use only by boot script
#
# written by Andrew Peace

OPERATION_REBOOT = -1
(
    OPERATION_NONE,
    OPERATION_INSTALL,
    OPERATION_UPGRADE,
    OPERATION_LOAD_DRIVER,
    OPERATION_RESTORE,
) = range(5)
