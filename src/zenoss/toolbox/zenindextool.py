##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

scriptVersion = "1.0.1"

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
from Products.Zuul.catalog.events import IndexingEvent
from ZODB.transact import transact
from zope.event import notify


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

@transact
def index_device(dev, dmd, log):
    try:
        notify(IndexingEvent(dev))
        dev.index_object(noips=True)
    except Exception as e:
        log.exception(e)
    for comp in dev.getDeviceComponentsNoIndexGen():
        try:
            notify(IndexingEvent(comp))
            comp.index_object()
        except Exception as e:
            log.exception(e)

def reindex_dmd_objects(name, type, dmd, log):
    try:
        inline_print("[%s] Reindexing %s ... " % (time.strftime("%Y-%m-%d %H:%M:%S"), name))
        if not (name == 'Devices'):
            object_reference = eval(type)
            object_reference.reIndex()
            print("finished")
            log.info("%s reIndex() completed successfully", name)
        else: # Special case for Devices, using method from altReindex ZEN-10793
            log.info("Reindexing Devices")
            output_count = 0
            for dev in dmd.Devices.getSubDevicesGen_recursive():
                index_device(dev, dmd, log)
                output_count += 1
                dev._p_deactivate()
                transaction.commit()
                
                if (output_count % 10) == 0:
                    # sync after 10 devices
                    dmd._p_jar.sync()
                    
                    if (output_count % 100) == 0:
                        log.debug("Device Reindex has passed %d devices" % (output_count))
                    inline_print("[%s] Reindexing %s ... %8d devices processed" %
                                 (time.strftime("%Y-%m-%d %H:%M:%S"), "Devices", output_count))
                
            inline_print("[%s] Reindexing %s ... finished                                    " %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), "Devices"))
            print ""
            log.info("%d Devices reindexed successfully" % (output_count))
        dmd._p_jar.sync()
        transaction.commit()
    except Exception as e:
        print " FAILED  (check log file for details)"
        log.error("%s.reIndex() failed" % (name))
        log.exception(e)


def parse_options():
    """Defines command-line options for script """

    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Reindexes top-level organizers. Documentation available at "
                                     "https://support.zenoss.com/hc/en-us/articles/203263689")

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    parser.add_argument("-l", "--list", action="store_true", default=False,
                        help="output all supported reIndex() types")
    parser.add_argument("-t", "--type", action="store", default="",
                        help="specify which type to reIndex()")

    return vars(parser.parse_args())


def main():
    '''Performs reindex call on different DMD categories (used to be a part of zencatalogscan)'''

    execution_start = time.time()
    cli_options = parse_options()
    log = configure_logging('zenindextool')
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

    # Else build list of catalogs, then process catalog(s) and perform reindex if --fix
    types_to_reIndex = {
        'Devices': 'dmd.Devices',
        'Events': 'dmd.Events',
        'Manufacturers': 'dmd.Manufacturers',
        'Networks': 'dmd.Networks',
        'Services': 'dmd.Services'
        }

    if cli_options['list'] or not cli_options['type'] :
    # Output list of present catalogs to the UI, perform no further operations
        print "List of dmd types that support reIndex() calls from this script:\n"
        print "\n".join(types_to_reIndex.keys())
        log.info("Zenreindextool finished - list of supported types output to CLI")
    else:
        if cli_options['type'] in types_to_reIndex.keys():
            reindex_dmd_objects(cli_options['type'], types_to_reIndex[cli_options['type']], dmd, log)
        else:
            print("Type '%s' unrecognized - unable to reIndex()" % (cli_options['type']))
            log.error("CLI input '%s' doesn't match recognized types" % (cli_options['type']))
            exit(1)

    # Print final status summary, update log file with termination block
    print("\n[%s] Execution finished in %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"),
                                                 datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zenindextool completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")


if __name__ == "__main__":
    main()
