#!/usr/bin/env python
#####################

scriptVersion = "1.0.4"

import Globals
import argparse
import sys
import os
import traceback
import logging
import socket
import time
import datetime
import transaction
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.Zuul.catalog.events import IndexingEvent
from ZODB.transact import transact
from zope.event import notify
from time import localtime, strftime

execution_start = time.time()
any_issue_detected = False
log_file_path = os.path.join(os.getenv("ZENHOME"), 'log', 'toolbox')
if not os.path.exists(log_file_path):
    os.makedirs(log_file_path)
log_file_name = os.path.join(os.getenv("ZENHOME"), 'log', 'toolbox', 'zencatalogscan.log')
logging.basicConfig(filename='%s' % (log_file_name),
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("zen.zencatalogscan")
print("\n[%s] Initializing zencatalogscan (detailed log at %s)\n" %
      (strftime("%Y-%m-%d %H:%M:%S", localtime()), log_file_name))
log.info("Initializing zencatalogscan")
dmd = ZenScriptBase(noopts=True, connect=True).dmd
log.info("Obtained ZenScriptBase connection")


def get_lock(process_name):
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        log.info("'zenoss.toolbox' lock acquired - continuing")
    except socket.error:
        print("[%s] Unable to acquire zenoss.toolbox socket lock - are other tools already running?\n" %
              (strftime("%Y-%m-%d %H:%M:%S", localtime())))
        log.error("'zenoss.tooblox' lock already exists - unable to acquire - exiting")
        return False
    return True


def progress_bar(message):
    sys.stdout.write("%s" % (message))
    sys.stdout.flush()


def scan_catalog(catalog_name, catalog_list, fix, max_cycles):
    """Scan through a catalog looking for broken references"""

    catalog = catalog_list[0]
    initial_catalog_size = catalog_list[1]

    print("[%s] Examining  %7d  '%s' Objects:" %
          (strftime("%Y-%m-%d %H:%M:%S", localtime()), initial_catalog_size, catalog_name))
    log.info("Examining %s catalog with %d objects" % (catalog_name, initial_catalog_size))

    global any_issue_detected
    current_cycle = 0
    number_of_issues = -1
    if not fix:
        max_cycles = 1

    while ((current_cycle < max_cycles) and (number_of_issues != 0)):
        try:
            brains = catalog()
        except Exception:
            raise

        catalog_size = len(brains)
        current_cycle += 1
        scanned_count = 0
        progress_bar_chunk_size = 1
        number_of_issues = 0

        if (catalog_size > 50):
            progress_bar_chunk_size = (catalog_size//50) + 1

        # ZEN-12165: show progress bar immediately before 'for' time overhead
        progress_bar("\r  Scanning  [%-50s] %3d%% " % ('='*0, 0))

        for brain in brains:
            if (scanned_count % progress_bar_chunk_size) == 0:
                chunk_number = scanned_count // progress_bar_chunk_size
                if number_of_issues > 0:
                    if fix:
                        progress_bar("\r  Cleaning  [%-50s] %3d%% [%d Issues Detected]" %
                                     ('='*chunk_number, 2*chunk_number, number_of_issues))
                    else:
                        progress_bar("\r  Scanning  [%-50s] %3d%% [%d Issues Detected]" %
                                     ('='*chunk_number, 2*chunk_number, number_of_issues))
                else:
                    progress_bar("\r  Scanning  [%-50s] %3d%% " % ('='*chunk_number, 2*chunk_number))

            scanned_count += 1

            try:
                test_reference = brain.getObject()
                test_reference._p_deactivate()
            except Exception:
                number_of_issues += 1
                any_issue_detected = True
                log.error("Catalog %s contains broken object %s" % (catalog_name, brain.getPath()))
                if fix:
                    log.info("Attempting to uncatalog %s" % (brain.getPath()))
                    transact(catalog.uncatalog_object)(brain.getPath())

        # Finish off the execution progress bar since we're complete with this pass
        if number_of_issues > 0:
            if fix:
                progress_bar("\r  Clean #%2.0d [%-50s] %3.0d%% [%d Issues Detected]\n" %
                             (current_cycle, '='*50, 100, number_of_issues))
            else:
                progress_bar("\r  WARNING   [%-50s] %3.0d%% [%d Issues Detected]\n" %
                             ('='*50, 100, number_of_issues))
        else:
            progress_bar("\r  Verified  [%-50s] %3.0d%%\n" % ('='*50, 100))

    transaction.abort()


def alt_reindex_devices(): 	# ZEN-10793: alternative for dmd.Devices.reIndex()
    output_count = 0
    for dev in dmd.Devices.getSubDevicesGen_recursive():
        if (output_count % 10) == 0:
            progress_bar("\rReindexing %s ... %8d devices processed" % ("Devices".rjust(13), output_count))
        notify(IndexingEvent(dev))
        dev.index_object(noips=True)
        for comp in dev.getDeviceComponentsNoIndexGen():
            notify(IndexingEvent(comp))
            comp.index_object()
        output_count += 1


def log_reindex_exception(type, exception):
    print(" FAILED (check %s)" % (log_file_name))
    log.error("%s failed to reindex successfully" % (type))
    log.exception(exception)


def reindex_dmd_objects():
    print("\n[%s] Reindexing dmd objects" % (strftime("%Y-%m-%d %H:%M:%S")))
    log.info("Reindexing dmd objects")
    try:
        progress_bar("\rReindexing %s ... " % "Devices".rjust(13))
        alt_reindex_devices()
        progress_bar("\rReindexing %s ... " % "Devices".rjust(13))
        print("finished                                 ")
        log.info("Devices reindexed successfully")
    except Exception, e:
        log_reindex_exception("Devices", e)
    try:
        progress_bar("\rReindexing %s ... " % "Events".rjust(13))
        dmd.Events.reIndex()
        print("finished")
        log.info("Events reindexed successfully")
    except Exception, e:
        log_reindex_exception("Events", e)
    try:
        progress_bar("\rReindexing %s ... " % "Manufacturers".rjust(13))
        dmd.Manufacturers.reIndex()
        print("finished")
        log.info("Manufacturers reindexed successfully")
    except Exception, e:
        log_reindex_exception("Manufacturers", e)
    try:
        progress_bar("\rReindexing %s ... " % "Networks".rjust(13))
        dmd.Networks.reIndex()
        print("finished")
        log.info("Networks reindexed successfully")
    except Exception, e:
        log_reindex_exception("Networks", e)


def build_catalog_dict():
    """Builds a list of catalogs present and > 0 objects"""

    catalogs_to_check = {
        'global_catalog': 'dmd.global_catalog',
        'Networks.ipSearch': 'dmd.Networks.ipSearch',
        'IPv6Networks.ipSearch': 'dmd.IPv6Networks.ipSearch',
        'Devices.deviceSearch': 'dmd.Devices.deviceSearch',
        'Services.serviceSearch': 'dmd.Services.serviceSearch',
        'ZenLinkManager.layer2_catalog': 'dmd.ZenLinkManager.layer2_catalog',
        'ZenLinkManager.layer3_catalog': 'dmd.ZenLinkManager.layer3_catalog',
        'maintenanceWindowSearch': 'dmd.maintenanceWindowSearch',
        'zenPackPersistence': 'dmd.zenPackPersistence',
        'Manufacturers.productSearch': 'dmd.Manufacturers.productSearch',
        'VMware.vmwareGuestSearch': 'dmd.Devices.VMware.vmwareGuestSearch',
        'CiscoUCS.ucsSearchCatalog': 'dmd.Devices.CiscoUCS.ucsSearchCatalog',
        'Storage.wwnCatalog': 'dmd.Devices.Storage.wwnCatalog',
        'Storage.iqnCatalog': 'dmd.Devices.Storage.iqnCatalog',
        'vCloud.vCloudVMSearch': 'dmd.Devices.vCloud.vCloudVMSearch',
        'vSphere.lunCatalog': 'dmd.Devices.vSphere.lunCatalog',
        'vSphere.vnicCatalog': 'dmd.Devices.vSphere.vnicCatalog',
        'vSphere.pnicCatalog': 'dmd.Devices.vSphere.pnicCatalog',
        'CloudStack.HostCatalog': 'dmd.Devices.CloudStack.HostCatalog',
        'CloudStack.RouterVMCatalog': 'dmd.Devices.CloudStack.RouterVMCatalog',
        'CloudStack.SystemVMCatalog': 'dmd.Devices.CloudStack.SystemVMCatalog',
        'CloudStack.VirtualMachineCatalog': 'dmd.Devices.CloudStack.VirtualMachineCatalog',
        'XenServer.XenServerCatalog': 'dmd.Devices.XenServer.XenServerCatalog',
        'XenServer.PIFCatalog': 'dmd.Devices.XenServer.PIFCatalog',
        'XenServer.VIFCatalog': 'dmd.Devices.XenServer.VIFCatalog'
        }

    intermediate_catalog_dict = {}

    for catalog in catalogs_to_check.keys():
        log.info("Checking existence of the %s catalog" % (catalog))
        try:
            temp_brains = eval(catalogs_to_check[catalog])
            if len(temp_brains) > 0:
                intermediate_catalog_dict[catalog] = [eval(catalogs_to_check[catalog]), len(temp_brains)]
            else:
                log.info("Catalog %s exists but has no items -- skipping" % (catalog))
        except AttributeError:
            log.error("Catalog %s not found - skipping" % (catalog))
        except Exception, e:
            log.exception(e)

    transaction.abort()
    return intermediate_catalog_dict


def parse_options():
    """Defines command-line options for script """

    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans catalogs for references to missing objects. \
                                         Before using zencatalogscan you must first confirm both \
                                         zodbscan & findposkeyerrors return clean.")

    parser.add_argument("-l", "--list", action="store_true", default=False,
                        help="list all catalogs supported for scan")
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to remove any invalid references")
    parser.add_argument("-n", "--N", action="store", default="12", type=int,
                        help="maximum times to cycle (with --fix)")
    parser.add_argument("-c", "--catalog", action="store", default=False,
                        help="select a single catalog to scan/fix")
    return vars(parser.parse_args())


def main():
    """Parses options, defines catalogs, scans catalogs, reindexes (if fix)"""

    # Attempt to get the zenoss-toolbox lock before any actions performed
    if not get_lock("zenoss-toolbox"):
        sys.exit(1)

    global any_issue_detected
    cli_options = parse_options()
    catalog_dict = build_catalog_dict()

    if cli_options['list']:
        print "List of support Zenoss catalogs to examine:\n"
        for i in catalog_dict.keys():
            print i
        print ""
        log.info("zencatalogscan finished - list of supported catalogs output to CLI")
        return

    if cli_options['catalog']:
        if cli_options['catalog'] in catalog_dict.keys():
            scan_catalog(cli_options['catalog'], catalog_dict[cli_options['catalog']],
                         cli_options['fix'], cli_options['N'])
        else:
            print("Catalog '%s' unrecognized; run 'zencatalogscan -l' for supported catalogs" %
                  (cli_options['catalog']))
            log.error("CLI input '%s' doesn't match any recognized catalogs" % (cli_options['catalog']))
            return
    else:
        for catalog in catalog_dict.keys():
            scan_catalog(catalog, catalog_dict[catalog], cli_options['fix'], cli_options['N'])

    if cli_options['fix']:
        reindex_dmd_objects()

    print("\n[%s] Execution finished in %s\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()),
                                                 datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zencatalogscan completed in %1.2f seconds" % (time.time() - execution_start))

    if any_issue_detected and not cli_options['fix']:
        print("** WARNING ** Issues were detected - Consult KB article #216 at")
        print("      http://support.zenoss.com/ics/support/KBAnswer.asp?questionID=216\n")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
