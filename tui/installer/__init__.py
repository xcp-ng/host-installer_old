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

is_not_restore_fn = lambda a: a['install-type'] != constants.INSTALL_TYPE_RESTORE
is_using_remote_media_fn = lambda a: 'source-media' in a and a['source-media'] in ['url', 'nfs']

def need_networking(answers):
    if 'source-media' in answers and \
           answers['source-media'] in ['url', 'nfs']:
        return True
    if 'installation-to-overwrite' in answers:
        settings = answers['installation-to-overwrite'].readSettings()
        return (settings['master'] != None)
    return False

def get_main_source_location_sequence():
    uis = tui.installer.screens
    Step = uicontroller.Step

    return [
        Step(tui.repo.select_repo_source,
             args=["Select Installation Source", "Please select the type of source you would like to use for this installation"],
             predicates=[is_not_restore_fn]),
        Step(uis.setup_runtime_networking,
             predicates=[need_networking]),
        Step(tui.repo.get_source_location,
             args=[True, True],
             predicates=[is_using_remote_media_fn]),
        Step(tui.repo.verify_source, args=['installation', True], predicates=[is_not_restore_fn])
    ]

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
            (not answers.has_key('installation-to-overwrite') or \
                 not answers['installation-to-overwrite'].settingsAvailable())

    has_multiple_nics = lambda a: len(a['network-hardware'].keys()) > 1

    is_reinstall_fn = lambda a: a['install-type'] == constants.INSTALL_TYPE_REINSTALL
    is_clean_install_fn = lambda a: a['install-type'] == constants.INSTALL_TYPE_FRESH

    def requires_backup(answers):
        return answers.has_key("installation-to-overwrite") and \
               upgrade.getUpgrader(answers['installation-to-overwrite']).requires_backup

    def optional_backup(answers):
        return answers.has_key("installation-to-overwrite") and \
               upgrade.getUpgrader(answers['installation-to-overwrite']).optional_backup

    def requires_repartition(answers):
        return 'installation-to-overwrite' in answers and \
           upgrade.getUpgrader(answers['installation-to-overwrite']).repartition

    def preserve_settings(answers):
        return answers.has_key('preserve-settings') and \
               answers['preserve-settings']
    not_preserve_settings = lambda a: not preserve_settings(a)

    def preserve_timezone(answers):
        if not_preserve_settings(answers):
            return False
        if not answers.has_key('installation-to-overwrite'):
            return False
        settings = answers['installation-to-overwrite'].readSettings()
        return settings.has_key('timezone') and not settings.has_key('request-timezone')
    not_preserve_timezone = lambda a: not preserve_timezone(a)

    def ha_enabled(answers):
        settings = {}
        if answers.has_key('installation-to-overwrite'):
            settings = answers['installation-to-overwrite'].readSettings()
        return settings.has_key('ha-armed') and settings['ha-armed']

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

    if not results.has_key('install-type'):
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
             predicates=[is_clean_install_fn])
    ] + get_main_source_location_sequence() + [
        Step(uis.master_not_upgraded,
             predicates=[out_of_order_pool_upgrade_fn]),
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
             args=[False, False],
             predicates=[more_media_fn, is_using_remote_media_fn]),
        Step(tui.repo.verify_source, args=['installation', False],
             predicates=[more_media_fn]),
        ]
    uicontroller.runSequence(seq, answers)
    return answers

def reconfigure_source_location_sequence(answers):
    uicontroller.runSequence(get_main_source_location_sequence(), answers)
    return answers
