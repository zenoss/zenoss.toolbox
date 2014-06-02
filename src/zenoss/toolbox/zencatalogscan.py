#!/usr/bin/env python
#####################

scriptVersion = "1.0.2"

import Globals
import argparse
import sys
import traceback
import time
import logging
import os
import datetime
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.CMFCore.utils import getToolByName
from Products.Zuul.catalog.events import IndexingEvent
from ZODB.transact import transact
from zope.event import notify

executionStart = time.time()
log_file_name = '%s/log/zencatalogscan.log' % (os.getenv("ZENHOME"))
logging.basicConfig(filename='%s' % (log_file_name),
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("zen.zencatalogscan")
print "Initializing zencatalogscan..."
log.info("Initializing")
dmd = ZenScriptBase(noopts=True, connect=True).dmd
summaryMsg = []
issueDetected = False


def scan_catalog(catalogName, catalog, fix, maxCycles):
    """Scan through a catalog looking for broken references"""

    try:
        brains = catalog()
    except AttributeError:
        print "{} not found.  skipping....".format(catalog)
    except Exception:
        raise

    sizeOfCatalog = len(brains)
    print "Examining %s [%s objects]:" % (catalogName, str(sizeOfCatalog))
    log.info("Examining %s catalog with %d objects" % (catalogName, sizeOfCatalog))

    global issueDetected
    currentCycle = 0
    numberOfIssues = -1
    highestIssues = 0
    lowestIssues = -1
    if not fix:
        maxCycles = 1

    localIssueFound = False

    while ((currentCycle < maxCycles) and (numberOfIssues != 0)):
    ## Begin while loop

        try:
            brains = catalog()
        except AttributeError:
            print "{} not found.  skipping....".format(catalog)
        except Exception:
            raise

        sizeOfCatalog = len(brains)

        currentCycle += 1
        scannedCount = 0
        progressBarChunkSize = 1
        objectsWithIssues = []

        if (sizeOfCatalog > 50):
            progressBarChunkSize = (sizeOfCatalog//50) + 1

        for brain in brains:
            if (scannedCount % progressBarChunkSize) == 0:
                chunkNumber = scannedCount // progressBarChunkSize
                if len(objectsWithIssues) > 0:
                    if fix:
                        sys.stdout.write("\r  Cleaning  [%-50s] %3.0d%% [%d Issues Detected]" %
                                         ('='*chunkNumber, 2*chunkNumber, len(objectsWithIssues)))
                    else:
                        sys.stdout.write("\r  Scanning  [%-50s] %3.0d%% [%d Issues Detected]" %
                                         ('='*chunkNumber, 2*chunkNumber, len(objectsWithIssues)))
                else:
                    sys.stdout.write("\r  Scanning  [%-50s] %3.0d%% " %
                                     ('='*chunkNumber, 2*chunkNumber))
                sys.stdout.flush()
            scannedCount += 1
            try:
                testReference = brain.getObject()
            except Exception:
                objectsWithIssues.append(brain.getPath())
                issueDetected = True
                localIssueFound = True
                log.error("Catalog %s contains broken object %s" % (catalogName, brain.getPath()))
                if fix:
                    transact(catalog.uncatalog_object)(brain.getPath())
                    log.info("Attempting to uncatalog %s" % (brain.getPath()))

        numberOfIssues = len(objectsWithIssues)

        # Finish off the execution progress bar since we're finished
        if len(objectsWithIssues) > 0:
            if fix:
                sys.stdout.write("\r  Clean #%2.0d [%-50s] %3.0d%% [%d Issues Detected]" %
                                 (currentCycle, '='*50, 100, len(objectsWithIssues)))
            else:
                sys.stdout.write("\r  WARNING   [%-50s] %3.0d%% [%d Issues Detected]" %
                                 ('='*50, 100, len(objectsWithIssues)))
        else:
            sys.stdout.write("\r  Verified  [%-50s] %3.0d%% " % ('='*50, 100))
        sys.stdout.flush()
        print ""

        # Append appropriate summaryMsg for this cycle 
        if not fix:
            if len(objectsWithIssues) > 0:
                summaryMsg.append("** WARNING ** %s catalog had %d issues" %
                                  (catalogName, len(objectsWithIssues)))
            else:
                summaryMsg.append("Verified %s catalog (no issues detected)" %
                                  (catalogName))
        else:
            if len(objectsWithIssues) > 0:
                if currentCycle == 1:
                    summaryMsg.append("Repairing %s catalog (issues were detected)" %
                                      (catalogName))
                summaryMsg.append("  Pass %d - Attempted repair for %s issues in %s catalog" %
                                  (currentCycle, len(objectsWithIssues), catalogName))
            else:
                if localIssueFound:
                    summaryMsg.append("  Verified %s catalog (no issues detected)" %
                                      (catalogName))
                else:
                    summaryMsg.append("Verified %s catalog (no issues detected)" %
                                      (catalogName))

    ## End While Loop


def altReindex_Devices():
# ZEN-10793: alternative for dmd.Devices.reIndex() that could fail on some systems
    for dev in dmd.Devices.getSubDevicesGen_recursive():
        notify(IndexingEvent(dev))
        dev.index_object(noips=True)
        for comp in dev.getDeviceComponentsNoIndexGen():
            notify(IndexingEvent(comp))
            comp.index_object()


def reindex_DMD():
    try:    # reindex Devices
        sys.stdout.write("\rReindexing %s ... " % "Devices".rjust(13))
        sys.stdout.flush()
        altReindex_Devices()
        print("finished")
        log.info("Devices reindexed successfully")
    except Exception, e:
        print(" FAILED  (check %s)" % (log_file_name))
        summaryMsg.append("EXCEPTION reindexing Devices (see %s)" % (log_file_name))
        log.error("Devices failed to reindex successfully")
        log.exception(e)

    try:    # reindex Events
        sys.stdout.write("\rReindexing %s ... " % "Events".rjust(13))
        sys.stdout.flush()
        dmd.Events.reIndex()
        print("finished")
        log.info("Events reindexed successfully")
    except Exception, e:
        print(" FAILED  (check %s)" % (log_file_name))
        summaryMsg.append("EXCEPTION reindexing Events (see %s)" % (log_file_name))
        log.error("Events failed to reindex successfully")
        log.exception(e)

    try:    # reindex Manufacturers
        sys.stdout.write("\rReindexing %s ... " % "Manufacturers".rjust(13))
        sys.stdout.flush()
        dmd.Manufacturers.reIndex()
        print("finished")
        log.info("Manufacturers reindexed successfully")
    except Exception, e:
        print(" FAILED  (check %s)" % (log_file_name))
        summaryMsg.append("EXCEPTION reindexing Manufacturers (see %s)" % (log_file_name))
        log.error("Manufacturers failed to reindex successfully")
        log.exception(e)

    try:    # reindex Networks
        sys.stdout.write("\rReindexing %s ... " % "Networks".rjust(13))
        sys.stdout.flush()
        dmd.Networks.reIndex()
        print("finished")
        log.info("Networks reindexed successfully")
    except Exception, e:
        print(" FAILED  (check %s)" % (log_file_name))
        summaryMsg.append("EXCEPTION reindexing Networks (see %s)" % (log_file_name))
        log.error("Networks failed to reindex successfully")
        log.exception(e)


def parse_options():
    """Defines and parses command-line options for script """
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
    """Scans catalogs, if fix: fixes and reindexes, and prints a final summary"""
    cmdLineOptions = parse_options()
    catalogDict = {
        'global_catalog': dmd.global_catalog,
        'IPv4.ipSearch': dmd.Networks.ipSearch,
        'IPv6.ipSearch': dmd.IPv6Networks.ipSearch,
        'deviceSearch': dmd.Devices.deviceSearch,
        'serviceSearch': dmd.Services.serviceSearch,
        'layer2_catalog': dmd.ZenLinkManager.layer2_catalog,
        'layer3_catalog': dmd.ZenLinkManager.layer3_catalog,
        'maintenanceWindowSearch': dmd.maintenanceWindowSearch,
        'zenPackPersistence': dmd.zenPackPersistence,
        'productSearch': dmd.Manufacturers.productSearch
        }

    if cmdLineOptions['list']:
        print "List of available Zenoss catalogs to scan:"
        print ""
        for i in catalogDict.keys():
            print i
        print ""
        log.info("List of supported catalogs output to stdout - script finished")
        return

    if cmdLineOptions['catalog']:
        if cmdLineOptions['catalog'] in catalogDict.keys():
            print("Processing Specific Catalog...")
            scan_catalog(cmdLineOptions['catalog'], catalogDict[cmdLineOptions['catalog']],
                         cmdLineOptions['fix'], cmdLineOptions['N'])
        else:
            print("Catalog '%s' unrecognized; run 'zencatalogscan -l' for supported catalogs" %
                  (cmdLineOptions['catalog']))
            log.error("Catalog '%s' unrecognized" % (cmdLineOptions['catalog']))
            return
    else:
        print("Processing Catalogs...\n")
        for catalog in catalogDict.keys():
            scan_catalog(catalog, catalogDict[catalog], cmdLineOptions['fix'], cmdLineOptions['N'])

    if cmdLineOptions['fix']:
        print "\nReindexing dmd Objects...\n"
        summaryMsg.append("")
        log.info("Reindexing dmd objects")
        reindex_DMD()

    print("\nSummary (completed in %s):\n" %
          (str(datetime.timedelta(seconds=(int(time.time() - executionStart))))))
    for item in summaryMsg:
        print "  %s" % (item)
    print ""

    if issueDetected and not cmdLineOptions['fix']:
        print("** WARNING ** Issues were detected - Confirm zodbscan and findposkeyerror are clean")
        print("              and then repair catalogs with 'zencatalogscan -f'")

    log.info("ZenCatalogScan completed in %1.2f seconds" % (time.time() - executionStart))


if __name__ == "__main__":
    main()
