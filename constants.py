# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Functions to perform the XE installation
#
# written by Andrew Peace & Mark Nijmeijer

import version
import string
import random

# exit status
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USER_CANCEL = 2

# install types:
INSTALL_TYPE_FRESH = "fresh"
INSTALL_TYPE_REINSTALL = "reinstall"
INSTALL_TYPE_RESTORE = "restore"

# sr types:
SR_TYPE_LVM = "lvm"
SR_TYPE_EXT = "ext"

# partition schemes:
PARTITION_DOS = "DOS"
PARTITION_GPT = "GPT"

# bootloader locations:
BOOT_LOCATION_MBR = "mbr"
BOOT_LOCATION_PARTITION = "partition"

# The lowest LBA that a partition can start at if installing the bootloader
# to the MBR (applies to legacy mode with DOS partition type only).
LBA_PARTITION_MIN = 63

# target boot mode:
TARGET_BOOT_MODE_LEGACY = "legacy"
TARGET_BOOT_MODE_UEFI = "uefi"

# first partition preservation:
PRESERVE_IF_UTILITY = "if-utility"
UTILITY_PARTLABEL = "DELLUTILITY"

# network backend types:
NETWORK_BACKEND_BRIDGE = "bridge"
NETWORK_BACKEND_VSWITCH = "openvswitch"
NETWORK_BACKEND_DEFAULT = NETWORK_BACKEND_VSWITCH

# Old name for openvswitch backend, for use in answerfile and on upgrade only
NETWORK_BACKEND_VSWITCH_ALT = "vswitch"

# error strings:
def error_string(error, logname, with_hd):
    (
        ERROR_STRING_UNKNOWN_ERROR_WITH_HD,
        ERROR_STRING_UNKNOWN_ERROR_WITHOUT_HD,
        ERROR_STRING_KNOWN_ERROR
    ) = range(3)

    ERROR_STRINGS = {
        ERROR_STRING_UNKNOWN_ERROR_WITH_HD: "An unrecoverable error has occurred.  The details of the error can be found in the log file, which has been written to /tmp/%s (and /root/%s on your hard disk if possible).",
        ERROR_STRING_UNKNOWN_ERROR_WITHOUT_HD: "An unrecoverable error has occurred.  The details of the error can be found in the log file, which has been written to /tmp/%s.",
        ERROR_STRING_KNOWN_ERROR: "An unrecoverable error has occurred.  The error was:\n\n%s\n"
    }

    if error == "":
        if with_hd:
            return ERROR_STRINGS[ERROR_STRING_UNKNOWN_ERROR_WITH_HD] % (logname, logname)
        else:
            return ERROR_STRINGS[ERROR_STRING_UNKNOWN_ERROR_WITHOUT_HD] % logname
    else:
        return ERROR_STRINGS[ERROR_STRING_KNOWN_ERROR] % error

# minimum hardware specs:
# memory checks should be done against MIN_SYSTEM_RAM_MB since libxc
# reports the total system ram after the Xen heap.  The UI should
# display the value given by MIN_SYSTEM_RAM_MB_RAW.
min_primary_disk_size_old = 12 #GB
min_primary_disk_size = 46 #GB
max_primary_disk_size_dos = 2047 #GB
MIN_SYSTEM_RAM_MB_RAW = 1024 # MB
MIN_SYSTEM_RAM_MB = MIN_SYSTEM_RAM_MB_RAW - 100

# Change this to True to enable GPT partitioning instead of DOS partitioning
GPT_SUPPORT = True

# filesystems and partitions (sizes in MB):
boot_size = 512
root_mbr_size_old = 4096
root_mbr_size = 18432
root_gpt_size_old = 3584
root_gpt_size = 17920
root_size_old = max(root_mbr_size_old, root_gpt_size_old)  # used for free space calculations
root_size = max(root_mbr_size, root_gpt_size)  # used for free space calculations
backup_size_old = 4096
backup_size = 18432
swap_file_size = 512
swap_size = 1024
logs_size = 4096
logs_free_space = 20

# filesystems and partitions types:
bootfs_type = 'vfat'
rootfs_type = 'ext3'
logsfs_type = 'ext3'

# filesystems and partitions labels:
bootfs_label = "BOOT-%s"
rootfs_label = "root-%s"
swap_file = '/var/swap/swap.001'
swap_label = 'swap-%s'
logsfs_label_prefix = 'logs-'
logsfs_label = logsfs_label_prefix + '%s'

MIN_PASSWD_LEN=6

# file locations - installer filesystem
EULA_PATH = "/opt/xensource/installer/EULA"
INSTALLER_DIR="/opt/xensource/installer"
timezone_data_file = '/opt/xensource/installer/timezones'
kbd_data_file = '/opt/xensource/installer/keymaps'
ANSWERFILE_PATH = '/tmp/answerfile'
ANSWERFILE_GENERATOR_PATH = '/tmp/answerfile_generator'
SCRIPTS_DIR = "/tmp/scripts"
EXTRA_SCRIPTS_DIR = "/tmp/extra-scripts"
defaults_data_file = '/opt/xensource/installer/defaults.json'
SYSFS_IBFT_DIR = "/sys/firmware/ibft"

# host filesystem - always absolute paths from root of install
# and never start with a '/', so they can be used safely with
# os.path.join.
ANSWERS_FILE = "upgrade_answers"
INVENTORY_FILE = "etc/xensource-inventory"
XENCOMMONS_FILE = "etc/sysconfig/xencommons"
OLD_BLOB_DIRECTORY = "var/xapi/blobs"
BLOB_DIRECTORY = "var/lib/xcp/blobs"

MAIN_REPOSITORY_NAME = 'xcp:main'
MAIN_REPOSITORY_GPG_KEY_FILE = '/opt/xensource/installer/RPM-GPG-KEY-xcpng'

MAIN_XS_REPOSITORY_NAME = 'xs:main'
INTERNAL_REPOS = [MAIN_XS_REPOSITORY_NAME, "xs:xenserver-transfer-vm", "xs:linux", "xcp:extras"]

FIRSTBOOT_DATA_DIR = "etc/firstboot.d/data"
INSTALLED_REPOS_DIR = "etc/xensource/installed-repos"
NETWORK_DB = "var/lib/xcp/networkd.db"
NETWORKD_DB = "usr/bin/networkd_db"
NET_SCR_DIR = "etc/sysconfig/network-scripts"
OLD_XAPI_DB = 'var/xapi/state.db'
XAPI_DB = 'var/lib/xcp/state.db'
CLUSTERD_CONF = 'var/opt/xapi-clusterd/db'

POST_INSTALL_SCRIPTS_DIR = "etc/xensource/scripts/install"

SYSLINUX_CFG = "syslinux.cfg"
ROLLING_POOL_DIR = "boot/installer"

HYPERVISOR_CAPS_FILE = "/sys/hypervisor/properties/capabilities"
SAFE_2_UPGRADE = "var/preserve/safe2upgrade"

# timer to exit installer after fatal error
AUTO_EXIT_TIMER = 10 * 1000

# bootloader timeout
BOOT_MENU_TIMEOUT = 50

# timeout used for multipath iscsi
MPATH_ISCSI_TIMEOUT = 15

ISCSI_NODES = 'var/lib/iscsi/nodes'

# prepare configuration for common criteria security
CC_PREPARATIONS = False

# list of dom0 services that will be disabled for common criteria preparation,
# and these can be overridden by answer file
SERVICES = ["sshd"]
