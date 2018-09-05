#!/bin/bash
# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this 
# copyrighted material is governed by and subject to terms and conditions 
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or 
# trademarks of XenSource Inc. in the United States and/or other countries.

SUPPORT_FILE="/tmp/support.tar.bz2"
echo "Collecting logs for submission to Technical Support..."
/usr/bin/python /opt/xensource/installer/xelogging.py
echo
echo "Logfiles have been collected. You can find them in ${SUPPORT_FILE}:"
echo
ls -la ${SUPPORT_FILE}
echo
echo "The contents of ${SUPPORT_FILE}:"
echo
tar jtvf ${SUPPORT_FILE}

