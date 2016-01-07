##############################################################################
#
# Copyright (C) Zenoss, Inc. 2016, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################
#!/opt/zenoss/bin/python

scriptVersion = "2.0.0"
scriptSummary = " - scans catalogs for broken references - WARNING: Before using with --fix " \
                "you MUST confirm zodbscan, findposkeyerror, and zenrelationscan return " \
                "no errors. "
documentationURL = "https://support.zenoss.com/hc/en-us/articles/203118075"


import argparse
import datetime
import Globals
import logging
import os
import sys
import time
import traceback
import transaction
import ZenToolboxUtils

from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from ZODB.transact import transact


def inline_print(message):
    '''Print message on a single line using sys.stdout.write, .flush'''
    sys.stdout.write("\r%s" % (message))
    sys.stdout.flush()


def scan_progress_message(done, fix, cycle, catalog, issues, chunk, log):
    '''Handle output to screen and logfile, remove output from scan_catalog logic'''
    # Logic for log file output messages based on done, issues
    if not done:
        log.debug("Scan of %s catalog is %2d%% complete" % (catalog, 2*chunk))
    else:
        if issues > 0:
            log.warning("Scanned %s - found %d issue(s)" % (catalog, issues))
        else:
            log.info("No issues found scanning: %s" % (catalog))
        log.debug("Scan of %s catalog is complete" % (catalog))
    # Logic for screen output messages based on done, issues, and fix
    if issues > 0:
        if fix:
            if not done:
                inline_print("[%s]  Cleaning  [%-50s] %3d%% [%d Issues Detected]" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk, 2*chunk, issues))
            else:
                inline_print("[%s]  Clean #%2.0d [%-50s] %3.0d%% [%d Issues Detected]\n" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), cycle, '='*50, 100, issues))
        else:
            if not done:
                inline_print("[%s]  Scanning  [%-50s] %3d%% [%d Issues Detected]" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk, 2*chunk, issues))
            else:
                inline_print("[%s]  WARNING   [%-50s] %3.0d%% [%d Issues Detected]\n" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100, issues))
    else:
        if not done:
            inline_print("[%s]  Scanning  [%-50s] %3d%% " %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk, 2*chunk))
        else:
            inline_print("[%s]  Verified  [%-50s] %3.0d%%\n" %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100))


def global_catalog_rids(catalog_name, catalog_list, fix, max_cycles, dmd, log, create_events):
    """Scan through global_catalog verifying consistency of rids"""

    catalog_reference = catalog_list[0]._catalog
    number_of_items = len(catalog_reference.paths)
    number_of_issues = -1
    current_cycle = 0
    if not fix:
        max_cycles = 1

    log.info("Examining global_catalog's ._catalog.paths for consistency against ._catalog.uids")
    print("[%s] Examining %-35s (%d Objects)" %
              (time.strftime("%Y-%m-%d %H:%M:%S"), "global_catalog RIDs consistency", number_of_items))

    while ((current_cycle < max_cycles) and (number_of_issues != 0)):
        number_of_items = len(catalog_reference.paths)
        if number_of_items > 50:
            progress_bar_chunk_size = (number_of_items//50) + 1
        number_of_issues = 0
        current_cycle += 1
        scanned_count = 0

        if (fix):
            log.info("Beginning cycle %d for global_catalog RIDs consistency", current_cycle)

        # ZEN-12165: show progress bar immediately before 'for' time overhead, before loading catalog
        scan_progress_message(False, fix, current_cycle, "global_catalog RIDs consistency", 0, 0, log)

        try:
            broken_rids = []

            for rid, path in catalog_reference.paths.iteritems():
                scanned_count += 1
                if (scanned_count % progress_bar_chunk_size) == 0:
                    chunk_number = scanned_count // progress_bar_chunk_size
                    scan_progress_message(False, fix, current_cycle, "global_catalog RIDs consistency", number_of_issues, chunk_number, log)
                if path not in catalog_reference.uids:
                    number_of_issues += 1
                    broken_rids.append(rid)

            if number_of_issues > 0:
                log.warning("global_catalog RIDs consistency detected %s issues with paths/data and uids", number_of_issues)
            else:
                log.info("global_catalog RIDs consistency detected %s issues with paths/data and uids", number_of_issues)

        except Exception, e:
            log.exception(e)

        scan_progress_message(True, fix, current_cycle, "global_catalog RIDs consistency", number_of_issues, chunk_number, log)
            
        if fix and (number_of_issues > 0):
            log.info("Attempting to correct issues found Paths/UIDs check found %s issues - attempting to remove", len(broken_rids))
            for item in broken_rids:
                try:
                    catalog_reference.paths.pop(item)
                    catalog_reference.data.pop(item)
                except:
                    pass

            catalog_reference._p_changed = True
            transaction.commit()
        else:
            # Final transaction.abort() to try and free up used memory
            log.debug("Calling transaction.abort() to minimize memory footprint")
            transaction.abort()

    if create_events:
        if number_of_issues > 0:
            eventSummaryMsg = "'%s' - %d Error(s) Detected (%d total items)" % \
                                  ('global_catalog_RIDs', number_of_issues, number_of_items)
            eventSeverity = 4
        else:
            eventSummaryMsg = "'%s' - No Errors Detected (%d total items)" % \
                                  ('global_catalog_RIDs', number_of_items)
            eventSeverity = 1

        dmd.ZenEventManager.sendEvent({
            'device'        : 'localhost',
            'summary'       : eventSummaryMsg,
            'message'       : eventSummaryMsg,
            'component'     : 'zenoss_toolbox',
            'severity'      : eventSeverity,
            'eventClass'    : '/Status',
            'eventKey'      : "global_catalog_RIDs",
            'dedupid'       : "zenoss_toolbox_zencatalogscan.global_catalog_RIDs",
            'eventClassKey' : "zenoss_toolbox_zencatalogscan",
            'details'       : "Consult https://support.zenoss.com/hc/en-us/articles/203118075 for additional information"
        })


def scan_catalog(catalog_name, catalog_list, fix, max_cycles, dmd, log, create_events):
    """Scan through a catalog looking for broken references"""

    # Fix for ZEN-14717 (only for global_catalog)
    if (catalog_name == 'global_catalog'):
        global_catalog_rids(catalog_name, catalog_list, fix, max_cycles, dmd, log, create_events)

    catalog = catalog_list[0]
    initial_catalog_size = catalog_list[1]
    number_of_issues = -1
    current_cycle = 0
    if not fix:
        max_cycles = 1

    print("[%s] Examining %-35s (%d Objects)" %
          (time.strftime("%Y-%m-%d %H:%M:%S"), catalog_name, initial_catalog_size))
    log.info("Examining %s catalog with %d objects" % (catalog_name, initial_catalog_size))

    while ((current_cycle < max_cycles) and (number_of_issues != 0)):
        number_of_issues = 0
        current_cycle += 1
        if (fix):
            log.info("Beginning cycle %d for catalog %s" % (current_cycle, catalog_name))
        scanned_count = 0
        progress_bar_chunk_size = 1

        # ZEN-12165: show progress bar immediately before 'for' time overhead, before loading catalog
        scan_progress_message(False, fix, current_cycle, catalog_name, 0, 0, log)

        try:
            brains = catalog()
            catalog_size = len(brains)
            if (catalog_size > 50):
                progress_bar_chunk_size = (catalog_size//50) + 1
        except Exception:
            raise

        for brain in brains:
            scanned_count += 1
            if (scanned_count % progress_bar_chunk_size) == 0:
                chunk_number = scanned_count // progress_bar_chunk_size
                scan_progress_message(False, fix, current_cycle, catalog_name, number_of_issues, chunk_number, log)
            try:
                test_reference = brain.getObject()
                test_reference._p_deactivate()
            except Exception:
                number_of_issues += 1
                object_path_string = brain.getPath()
                log.error("Catalog %s contains broken object %s" % (catalog_name, object_path_string))
                if fix:
                    log.info("Attempting to uncatalog %s" % (object_path_string))
                    try:
                        transact(catalog.uncatalog_object)(object_path_string)
                    except Exception as e:
                        log.exception(e)

        # Final transaction.abort() to try and free up used memory
        log.debug("Calling transaction.abort() to minimize memory footprint")
        transaction.abort()

        scan_progress_message(True, fix, current_cycle, catalog_name, number_of_issues, chunk_number, log)

    if create_events:
        if number_of_issues > 0:
            eventSummaryMsg = "'%s' - %d Error(s) Detected (%d total items)" % (catalog_name, number_of_issues, initial_catalog_size)
            eventSeverity = 4  
        else:
            eventSummaryMsg = "'%s' - No Errors Detected (%d total items)" % (catalog_name, initial_catalog_size)
            eventSeverity = 1
     
        dmd.ZenEventManager.sendEvent({
            'device'        : 'localhost',
            'summary'       : eventSummaryMsg,
            'message'       : eventSummaryMsg,
            'component'     : 'zenoss_toolbox',
            'severity'      : eventSeverity,
            'eventClass'    : '/Status',
            'eventKey'      : "%s" % (catalog_name),
            'dedupid'       : "zenoss_toolbox_zencatalogscan.%s" % (catalog_name),
            'eventClassKey' : "zenoss_toolbox_zencatalogscan",
            'details'       : "Consult https://support.zenoss.com/hc/en-us/articles/203118075 for additional information"
        })

    return (number_of_issues != 0)


def build_catalog_dict(dmd, log):
    """Builds a list of catalogs present and > 0 objects"""

    catalogs_to_check = {
        'CiscoUCS.ucsSearchCatalog': 'dmd.Devices.CiscoUCS.ucsSearchCatalog',
        'CloudStack.HostCatalog': 'dmd.Devices.CloudStack.HostCatalog',
        'CloudStack.RouterVMCatalog': 'dmd.Devices.CloudStack.RouterVMCatalog',
        'CloudStack.SystemVMCatalog': 'dmd.Devices.CloudStack.SystemVMCatalog',
        'CloudStack.VirtualMachineCatalog': 'dmd.Devices.CloudStack.VirtualMachineCatalog',
        'Devices.deviceSearch': 'dmd.Devices.deviceSearch',
        'Devices.searchRRDTemplates': 'dmd.Devices.searchRRDTemplates',
        'Events.eventClassSearch': 'dmd.Events.eventClassSearch',
        'global_catalog': 'dmd.global_catalog',
        'HP.Proliant.deviceSearch': 'dmd.Devices.Server.HP.Proliant.deviceSearch',
        'IPv6Networks.ipSearch': 'dmd.IPv6Networks.ipSearch',
        'JobManager.job_catalog': 'dmd.JobManager.job_catalog',
        'Layer2.macs_catalog': 'dmd.Devices.macs_catalog',
        'maintenanceWindowSearch': 'dmd.maintenanceWindowSearch',
        'Manufacturers.productSearch': 'dmd.Manufacturers.productSearch',
        'Mibs.mibSearch': 'dmd.Mibs.mibSearch',
        'Networks.ipSearch': 'dmd.Networks.ipSearch',
        'Services.serviceSearch': 'dmd.Services.serviceSearch',
        'Storage.iqnCatalog': 'dmd.Devices.Storage.iqnCatalog',
        'Storage.wwnCatalog': 'dmd.Devices.Storage.wwnCatalog',
        'vCloud.vCloudVMSearch': 'dmd.Devices.vCloud.vCloudVMSearch',
        'VMware.vmwareGuestSearch': 'dmd.Devices.VMware.vmwareGuestSearch',
        'vSphere.lunCatalog': 'dmd.Devices.vSphere.lunCatalog',
        'vSphere.pnicCatalog': 'dmd.Devices.vSphere.pnicCatalog',
        'vSphere.vnicCatalog': 'dmd.Devices.vSphere.vnicCatalog',
        'XenServer.PIFCatalog': 'dmd.Devices.XenServer.PIFCatalog',
        'XenServer.VIFCatalog': 'dmd.Devices.XenServer.VIFCatalog',
        'XenServer.XenServerCatalog': 'dmd.Devices.XenServer.XenServerCatalog',
        'ZenLinkManager.layer2_catalog': 'dmd.ZenLinkManager.layer2_catalog',
        'ZenLinkManager.layer3_catalog': 'dmd.ZenLinkManager.layer3_catalog',
        'zenPackPersistence': 'dmd.zenPackPersistence'
    }

    log.debug("Checking %d supported catalogs for (presence, not empty)" % (len(catalogs_to_check)))

    intermediate_catalog_dict = {}

    for catalog in catalogs_to_check.keys():
        try:
            temp_brains = eval(catalogs_to_check[catalog])
            if len(temp_brains) > 0:
                log.debug("Catalog %s exists, has items - adding to list" % (catalog))
                intermediate_catalog_dict[catalog] = [eval(catalogs_to_check[catalog]), len(temp_brains)]
            else:
                log.debug("Skipping catalog %s - exists but has no items" % (catalog))
        except AttributeError:
            log.debug("Skipping catalog %s - catalog not found" % (catalog))
        except Exception, e:
            log.exception(e)

    return intermediate_catalog_dict


def main():
    """Scans catalogs for broken references.  If --fix, attempts to remove broken references."""

    execution_start = time.time()
    scriptName = os.path.basename(__file__).split('.')[0]
    parser = ZenToolboxUtils.parse_options(scriptVersion, scriptName + scriptSummary + documentationURL)
    # Add in any specific parser arguments for %scriptName
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to remove any invalid references")
    parser.add_argument("-n", "--cycles", action="store", default="12", type=int,
                        help="maximum times to cycle (with --fix)")
    parser.add_argument("-l", "--list", action="store_true", default=False,
                        help="output all supported catalogs")
    parser.add_argument("-c", "--catalog", action="store", default="",
                        help="only scan/fix specified catalog")
    parser.add_argument("-e", "--events", action="store_true", default=False,
                        help="create Zenoss events with status")
    cli_options = vars(parser.parse_args())
    log, logFileName = ZenToolboxUtils.configure_logging(scriptName, scriptVersion, cli_options['tmpdir'])
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    print "\n[%s] Initializing %s v%s (detailed log at %s)" % \
          (time.strftime("%Y-%m-%d %H:%M:%S"), scriptName, scriptVersion, logFileName)

    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not ZenToolboxUtils.get_lock("zenoss.toolbox", log):
        sys.exit(1)

    # Obtain dmd ZenScriptBase connection
    dmd = ZenScriptBase(noopts=True, connect=True).dmd
    log.debug("ZenScriptBase connection obtained")

    any_issue = False
    unrecognized_catalog = False

    # Build list of catalogs, then process catalog(s) and perform reindex if --fix
    present_catalog_dict = build_catalog_dict(dmd, log)
    if cli_options['list']:
    # Output list of present catalogs to the UI, perform no further operations
        print "List of supported Zenoss catalogs to examine:\n"
        print "\n".join(present_catalog_dict.keys())
        log.info("Zencatalogscan finished - list of supported catalogs output to CLI")
    else:
    # Scan through catalog(s) depending on --catalog parameter
        if cli_options['catalog']:
            if cli_options['catalog'] in present_catalog_dict.keys():
            # Catalog provided as parameter is present - scan just that catalog
                any_issue = scan_catalog(cli_options['catalog'], present_catalog_dict[cli_options['catalog']],
                                         cli_options['fix'], cli_options['cycles'], dmd, log, cli_options['events'])
            else:
                unrecognized_catalog = True
                print("Catalog '%s' unrecognized - unable to scan" % (cli_options['catalog']))
                log.error("CLI input '%s' doesn't match recognized catalogs" % (cli_options['catalog']))
        else:
        # Else scan for all catalogs in present_catalog_dict
            for catalog in present_catalog_dict.keys():
                any_issue = scan_catalog(catalog, present_catalog_dict[catalog], cli_options['fix'],
                                         cli_options['cycles'], dmd, log, cli_options['events']) or any_issue

    # Print final status summary, update log file with termination block
    print("\n[%s] Execution finished in %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"),
                                                 datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zencatalogscan completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")

    if cli_options['events']:
        if any_issue: 
            eventSummaryMsg = "zencatalogscan encoutered errors (took %1.2f seconds)" % (time.time() - execution_start)
            eventSeverity = 4 
        else:
            eventSummaryMsg = "zencatalogscan completed without errors (took %1.2f seconds)" % (time.time() - execution_start)
            eventSeverity = 2

        dmd.ZenEventManager.sendEvent({
            'device'        : 'localhost',
            'summary'       : eventSummaryMsg,
            'message'       : eventSummaryMsg,
            'component'     : 'zenoss_toolbox',
            'severity'      : eventSeverity,
            'eventClass'    : '/Status',
            'eventKey'      : "execution_status",
            'dedupid'       : "zenoss_toolbox_zencatalogscan.execution_status",
            'eventClassKey' : "zenoss_toolbox_zencatalogscan",
            'details'       : "Consult https://support.zenoss.com/hc/en-us/articles/203118075 for additional information"
        })

    if any_issue and not cli_options['fix']:
        print("** WARNING ** Issues were detected - Consult KB article at")
        print("      https://support.zenoss.com/hc/en-us/articles/203118075\n")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
