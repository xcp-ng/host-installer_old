# Copyright (c) 2005-2006 XenSource, Inc. All use and distribution of this
# copyrighted material is governed by and subject to terms and conditions
# as licensed by XenSource, Inc. All other rights reserved.
# Xen, XenSource and XenEnterprise are either registered trademarks or
# trademarks of XenSource Inc. in the United States and/or other countries.

###
# XEN CLEAN INSTALLER
# Installer TUI sequence definitions
#
# written by Andrew Peace

import tui.installer.screens
import tui.progress
import tui.repo
import uicontroller
from uicontroller import SKIP_SCREEN, EXIT, LEFT_BACKWARDS, RIGHT_FORWARDS, REPEAT_STEP
import hardware
import netutil
import repository
import constants
import upgrade
import product
import diskutil
from disktools import *
import version
import xmlrpclib

from snack import *

def need_networking(answers):
    if 'source-media' in answers and \
           answers['source-media'] in ['url', 'nfs']:
        return True
    if 'installation-to-overwrite' in answers:
        settings = answers['installation-to-overwrite'].readSettings()
        return (settings['master'] is not None)
    return False

is_using_remote_media_fn = lambda a: 'source-media' in a and a['source-media'] in ['url', 'nfs']

def runMainSequence(results, ram_warning, vt_warning, suppress_extra_cd_dialog):
    """ Runs the main installer sequence and updates results with a
    set of values ready for the backend. """
    uis = tui.installer.screens
    Step = uicontroller.Step

    def only_unupgradeable_products(answers):
        return len(answers['installed-products']) > 0 and \
               len(answers['upgradeable-products']) == 0 and \
               len(answers['backups']) == 0

    def upgrade_but_no_settings_predicate(answers):
        return answers['install-type'] == constants.INSTALL_TYPE_REINSTALL and \
            ('installation-to-overwrite' not in answers or \
                 not answers['installation-to-overwrite'].settingsAvailable())

    has_multiple_nics = lambda a: len(a['network-hardware'].keys()) > 1

    is_reinstall_fn = lambda a: a['install-type'] == constants.INSTALL_TYPE_REINSTALL
    is_clean_install_fn = lambda a: a['install-type'] == constants.INSTALL_TYPE_FRESH
    is_not_restore_fn = lambda a: a['install-type'] != constants.INSTALL_TYPE_RESTORE

    def requires_backup(answers):
        return "installation-to-overwrite" in answers and \
               upgrade.getUpgrader(answers['installation-to-overwrite']).requires_backup

    def optional_backup(answers):
        return "installation-to-overwrite" in answers and \
               upgrade.getUpgrader(answers['installation-to-overwrite']).optional_backup

    def requires_repartition(answers):
        return 'installation-to-overwrite' in answers and \
           upgrade.getUpgrader(answers['installation-to-overwrite']).repartition

    def preserve_settings(answers):
        return 'preserve-settings' in answers and \
               answers['preserve-settings']
    not_preserve_settings = lambda a: not preserve_settings(a)

    def preserve_timezone(answers):
        if not_preserve_settings(answers):
            return False
        if 'installation-to-overwrite' not in answers:
            return False
        settings = answers['installation-to-overwrite'].readSettings()
        return 'timezone' in settings and 'request-timezone' not in settings
    not_preserve_timezone = lambda a: not preserve_timezone(a)

    def ha_enabled(answers):
        settings = {}
        if 'installation-to-overwrite' in answers:
            settings = answers['installation-to-overwrite'].readSettings()
        return 'ha-armed' in settings and settings['ha-armed']

    def out_of_order_pool_upgrade_fn(answers):
        if 'installation-to-overwrite' not in answers:
            return False

        ret = False
        settings = answers['installation-to-overwrite'].readSettings()
        if settings['master']:
            if not netutil.networkingUp():
                pass

            try:
                s = xmlrpclib.Server("http://"+settings['master'])
                session = s.session.slave_login("", settings['pool-token'])["Value"]
                pool = s.pool.get_all(session)["Value"][0]
                master = s.pool.get_master(session, pool)["Value"]
                software_version = s.host.get_software_version(session, master)["Value"]
                s.session.logout(session)

                # compare versions
                master_ver = product.Version.from_string(software_version['product_version'])
                if master_ver < product.THIS_PRODUCT_VERSION:
                    ret = True
            except:
                pass

        return ret

    if 'install-type' not in results:
        results['install-type'] = constants.INSTALL_TYPE_FRESH
        results['preserve-settings'] = False

    seq = [
        Step(uis.welcome_screen),
        Step(uis.eula_screen),
        Step(uis.hardware_warnings,
             args=[ram_warning, vt_warning],
             predicates=[lambda _:(ram_warning or vt_warning)]),
        Step(uis.overwrite_warning,
             predicates=[only_unupgradeable_products]),
        Step(uis.get_installation_type,
             predicates=[lambda _:len(results['upgradeable-products']) > 0 or len(results['backups']) > 0]),
        Step(uis.upgrade_settings_warning,
             predicates=[upgrade_but_no_settings_predicate]),
        Step(uis.ha_master_upgrade,
             predicates=[is_reinstall_fn, ha_enabled]),
        Step(uis.remind_driver_repos,
             predicates=[is_reinstall_fn, preserve_settings]),
        Step(uis.backup_existing_installation,
             predicates=[is_reinstall_fn, optional_backup]),
        Step(uis.force_backup_screen,
             predicates=[is_reinstall_fn, requires_backup]),
        Step(uis.select_primary_disk,
             predicates=[is_clean_install_fn]),
        Step(uis.repartition_existing,
             predicates=[is_reinstall_fn, requires_repartition]),
        Step(uis.select_guest_disks,
             predicates=[is_clean_install_fn]),
        Step(uis.confirm_erase_volume_groups,
             predicates=[is_clean_install_fn]),
        Step(tui.repo.select_repo_source,
             args=["Select Installation Source", "Please select the type of source you would like to use for this installation"],
             predicates=[is_not_restore_fn]),
        Step(uis.setup_runtime_networking,
             predicates=[need_networking]),
        Step(uis.master_not_upgraded,
             predicates=[out_of_order_pool_upgrade_fn]),
        Step(tui.repo.get_source_location,
             args=[True],
             predicates=[is_using_remote_media_fn]),
        Step(tui.repo.verify_source, args=['installation', True], predicates=[is_not_restore_fn]),
        Step(uis.get_root_password,
             predicates=[is_not_restore_fn, not_preserve_settings]),
        Step(uis.get_admin_interface,
             predicates=[is_not_restore_fn, has_multiple_nics, not_preserve_settings]),
        Step(uis.get_admin_interface_configuration,
             predicates=[is_not_restore_fn, not_preserve_settings]),
        Step(uis.get_name_service_configuration,
             predicates=[is_not_restore_fn, not_preserve_settings]),
        Step(uis.get_timezone_region,
             predicates=[is_not_restore_fn, not_preserve_timezone]),
        Step(uis.get_timezone_city,
             predicates=[is_not_restore_fn, not_preserve_timezone]),
        Step(uis.get_time_configuration_method,
             predicates=[is_not_restore_fn, not_preserve_settings]),
        Step(uis.get_ntp_servers,
             predicates=[is_not_restore_fn, not_preserve_settings]),
        Step(uis.set_time,
             predicates=[is_not_restore_fn, not_preserve_settings]),
        Step(uis.confirm_installation),
        ]
    return uicontroller.runSequence(seq, results)

def more_media_sequence(answers):
    uis = tui.installer.screens
    Step = uicontroller.Step
    more_media_fn = lambda a: 'more-media' in a and a['more-media']

    seq = [
        Step(uis.use_extra_media),
        Step(tui.repo.select_repo_source,
             args=["Select Supplemental Pack source", "Please select the type of source you would like to use for this Supplemental Pack", False],
             predicates=[more_media_fn]),
        Step(uis.setup_runtime_networking,
             predicates=[more_media_fn, need_networking]),
        Step(tui.repo.get_source_location,
             args=[False],
             predicates=[more_media_fn, is_using_remote_media_fn]),
        Step(tui.repo.verify_source, args=['installation', False],
             predicates=[more_media_fn]),
        ]
    uicontroller.runSequence(seq, answers)
    return answers
