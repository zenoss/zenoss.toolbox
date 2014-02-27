#!/usr/bin/env python
#####################

scriptVersion = "1.0.0"

import Globals
import argparse
import sys
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from ZODB.transact import transact

summaryMsg = []


def scan_catalog(catalog, fix):
    """Scan through a catalog looking for broken references"""
    try:
        brains = catalog()
        sizeOfCatalog = len(brains)
        scannedCount = 0
        progressBarChunkSize = 1
        objectsWithIssues = []
        if not fix:
            print "Scanning   %s   [%s items]:" % (catalog.id, sizeOfCatalog)
        else:
            print "Fixing   %s   [%s items]:" % (catalog.id, sizeOfCatalog)

        if (sizeOfCatalog > 50):
            progressBarChunkSize = (sizeOfCatalog//50) + 1

        for brain in brains:
            if (scannedCount % progressBarChunkSize) == 0:
                sys.stdout.write('\r')
                chunkNumber = scannedCount // progressBarChunkSize
                sys.stdout.write("[%-50s] %d%%" % ('='*chunkNumber, 2*chunkNumber))
                sys.stdout.flush()
            scannedCount += 1
            try:
                testReference = brain.getObject()
            except Exception:
                objectsWithIssues.append(brain.getPath())
                if fix:
                    transact(catalog.uncatalog_object(brain.getPath()))

        # Finish off the execution progress bar since we're finished
        sys.stdout.write('\r')
        sys.stdout.write("[%-50s] %d%%" % ('='*50, 100))
        sys.stdout.flush()
        print ""

        if len(objectsWithIssues) == 0:
            summaryMsg.append("[Processed %s items in the %s catalog and found zero errors]"
                              % (scannedCount, catalog.id))
        else:
            summaryMsg.append("[Processed %s items in the %s catalog and found %s issues]"
                              % (scannedCount, catalog.id, len(objectsWithIssues)))
            for item in objectsWithIssues:
                summaryMsg.append("  %s" % (item))
    except AttributeError:
        print "{} not found.  skipping....".format(catalog)
    except Exception:
        raise


def parse_options():
    """Defines and parses command-line options for script """
    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans catalogs for references to  missing objects")
    parser.add_argument("run", help="execute the scan")
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to remove any invalid references")
    return vars(parser.parse_args())


def main():
    """Scans catalogs, reindexes if needed, and prints a summary"""
    cmdLineOptions = parse_options()
    dmd = ZenScriptBase(noopts=True, connect=True).dmd
    catalogList = [
        dmd.global_catalog,
        dmd.Networks.ipSearch,
        dmd.IPv6Networks.ipSearch,
        dmd.Devices.deviceSearch,
        dmd.Services.serviceSearch,
        dmd.ZenLinkManager.layer2_catalog,
        dmd.ZenLinkManager.layer3_catalog,
        dmd.maintenanceWindowSearch,
        dmd.zenPackPersistence,
        dmd.Manufacturers.productSearch
        ]

    for catalog in catalogList:
        scan_catalog(catalog, cmdLineOptions['fix'])

    if cmdLineOptions['fix']:
        print ""
        print "Reindexing dmd Objects..."
        dmd.Devices.reIndex()
        dmd.Events.reIndex()
        dmd.Manufacturers.reIndex()
        dmd.Networks.reIndex()

    print ""
    print "EXECUTION SUMMARY:"
    for item in summaryMsg:
        print item


if __name__ == "__main__":
    main()
