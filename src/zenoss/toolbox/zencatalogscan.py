##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

scriptVersion = "1.2.1"

import argparse
import datetime
import Globals
import logging
import os
import socket
import sys
import time
import traceback
import transaction

from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from ZODB.transact import transact


def configure_logging(scriptname):
    '''Configure logging for zenoss.toolbox tool usage'''

    # Confirm /tmp, $ZENHOME and check for $ZENHOME/log/toolbox (create if needed)
    if not os.path.exists('/tmp'):
        print "/tmp doesn't exist - aborting"
        exit(1)
    zenhome_path = os.getenv("ZENHOME")
    if not zenhome_path:
        print "$ZENHOME undefined - are you running as the zenoss user?"
        exit(1)
    log_file_path = os.path.join(zenhome_path, 'log', 'toolbox')
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)
    # Setup "trash" toolbox log file (needed for ZenScriptBase log overriding)
    logging.basicConfig(filename='/tmp/toolbox.log.tmp', filemode='w', level=logging.INFO)

    # Create full path filename string for logfile, create RotatingFileHandler
    toolbox_log = logging.getLogger("%s" % (scriptname))
    toolbox_log.setLevel(logging.INFO)
    log_file_name = os.path.join(zenhome_path, 'log', 'toolbox', '%s.log' % (scriptname))
    handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=8192*1024, backupCount=5)

    # Set logging.Formatter for format and datefmt, attach handler
    formatter = logging.Formatter('%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    toolbox_log.addHandler(handler)

    # Print initialization string to console, log status to logfile
    print("\n[%s] Initializing %s (detailed log at %s)\n" %
          (time.strftime("%Y-%m-%d %H:%M:%S"), scriptname, log_file_name))
    toolbox_log.info("Initializing %s" % (scriptname))
    return toolbox_log


def get_lock(process_name, log):
    '''Global lock function to keep multiple tools from running at once'''
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        log.debug("Acquired '%s' execution lock" % (process_name))
    except socket.error:
        print("[%s] Unable to acquire %s socket lock - are other tools already running?\n" %
              (time.strftime("%Y-%m-%d %H:%M:%S"), process_name))
        log.error("'%s' lock already exists - unable to acquire - exiting" % (process_name))
        log.info("############################################################")
        return False
    return True


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


def scan_catalog(catalog_name, catalog_list, fix, max_cycles, dmd, log):
    """Scan through a catalog looking for broken references"""

    catalog = catalog_list[0]
    initial_catalog_size = catalog_list[1]

    print("[%s] Examining %-35s (%d Objects)" %
          (time.strftime("%Y-%m-%d %H:%M:%S"), catalog_name, initial_catalog_size))
    log.info("Examining %s catalog with %d objects" % (catalog_name, initial_catalog_size))

    number_of_issues = -1
    current_cycle = 0
    if not fix:
        max_cycles = 1

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

    if number_of_issues > 0:
        return True
    return False


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


def parse_options():
    """Defines command-line options for script """

    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans catalogs for broken references. WARNING: Before using with --fix "
                                         "you must first confirm zodbscan, findposkeyerror, and zenrelationscan return "
                                         "clean. Documentation at "
                                         "https://support.zenoss.com/hc/en-us/articles/203118075")

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to remove any invalid references")
    parser.add_argument("-n", "--cycles", action="store", default="12", type=int,
                        help="maximum times to cycle (with --fix)")
    parser.add_argument("-l", "--list", action="store_true", default=False,
                        help="output all supported catalogs")
    parser.add_argument("-c", "--catalog", action="store", default="",
                        help="only scan/fix specified catalog")

    return vars(parser.parse_args())


def main():
    '''Scans catalogs for broken references.  If --fix, attempts to remove broken references.
       Builds list of available non-empty catalogs.  If --reindex, attempts dmd.reIndex().'''

    execution_start = time.time()
    cli_options = parse_options()
    log = configure_logging('zencatalogscan')
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not get_lock("zenoss.toolbox", log):
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
                                         cli_options['fix'], cli_options['cycles'], dmd, log)
            else:
                unrecognized_catalog = True
                print("Catalog '%s' unrecognized - unable to scan" % (cli_options['catalog']))
                log.error("CLI input '%s' doesn't match recognized catalogs" % (cli_options['catalog']))
        else:
        # Else scan for all catalogs in present_catalog_dict
            for catalog in present_catalog_dict.keys():
                any_issue = scan_catalog(catalog, present_catalog_dict[catalog], cli_options['fix'],
                                         cli_options['cycles'], dmd, log) or any_issue

    # Print final status summary, update log file with termination block
    print("\n[%s] Execution finished in %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"),
                                                 datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zencatalogscan completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")

    if any_issue and not cli_options['fix']:
        print("** WARNING ** Issues were detected - Consult KB article at")
        print("      https://support.zenoss.com/hc/en-us/articles/203118075\n")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
