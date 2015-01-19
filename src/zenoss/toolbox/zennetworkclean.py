##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

scriptVersion = "0.9.1"

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
from Acquisition import aq_parent

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


def scan_progress_message(done, fix, cycle, catalog, issues, total_number_of_issues, percentage, chunk, log):
    '''Handle output to screen and logfile, remove output from scan_catalog logic'''
    # Logic for log file output messages based on done, issues
    if not done:
        log.debug("Scan of %s catalog is %2d%% complete" % (catalog, 2*chunk))
    else:
        if issues > 0:
            log.warning("Scanned %s - found %d stale reference(s)" % (catalog, issues))
        else:
            log.info("No stale references found scanning: %s" % (catalog))
        log.debug("Scan of %s catalog is complete" % (catalog))
    # Logic for screen output messages based on done, issues, and fix
    if issues > 0:
        if fix:
            if not done:
                inline_print("[%s]  Cleaning  [%-50s] %3d%% [%d orphaned IPs are deleted]" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk, 2*chunk, issues))
            else:
                inline_print("[%s]  Clean #%2.0d [%-50s] %3.0d%% [%d orphaned IPs are deleted]\n" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), cycle, '='*50, 100, issues))
        else:
            if not done:
                inline_print("[%s]  Scanning  [%-50s] %3d%% [%d orphaned IPs are detected]" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk, 2*chunk, issues))
            else:
                inline_print("[%s]  WARNING   [%-50s] %3.0d%% [There are %d orphaned IPs (%.1f%%)]\n" %
                             (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100, issues, percentage))
    else:
        if not done:
            inline_print("[%s]  Scanning  [%-50s] %3d%% " %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk, 2*chunk))
        else:
            if (total_number_of_issues == 0):
                inline_print("[%s]  Verified  [%-50s] %3.0d%% [No issues] \n" %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100))
            else:
                inline_print("[%s]  Verified  [%-50s] %3.0d%% [%d orphaned IPs are deleted (%.1f%%)] \n" %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100, total_number_of_issues, percentage))

 
@transact
def scan_catalog(catalog_name, catalog_list, fix, max_cycles, dmd, log):
    """Scan through a catalog looking for broken references"""

    catalog = catalog_list[0]
    initial_catalog_size = catalog_list[1]

    print("[%s] Examining %-35s (%d Objects)" %
          (time.strftime("%Y-%m-%d %H:%M:%S"), catalog_name, initial_catalog_size))
    log.info("Examining %s catalog with %d objects" % (catalog_name, initial_catalog_size))

    number_of_issues = -1
    total_number_of_issues = 0
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
        scan_progress_message(False, fix, current_cycle, catalog_name, 0, 0, 0, 0, log)

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
                scan_progress_message(False, fix, current_cycle, catalog_name, number_of_issues, 0, 0, chunk_number, log)           
            try:
                ip = brain.getObject()
                if not ip.interface():
                    if not fix:
                        ip._p_deactivate()
                    raise Exception
                ip._p_deactivate()
            except Exception:
                number_of_issues += 1
                log.warning("Catalog %s contains orphaned object %s" % (catalog_name, ip.viewName()))
                if fix:
                    log.info("Attempting to delete %s" % (ip.viewName()))
                    try:
                        parent = aq_parent(ip)
                        parent._delObject(ip.id)
                        ip._p_deactivate()

                    except Exception as e:
                        log.exception(e)
        total_number_of_issues += number_of_issues
        percentage = total_number_of_issues*1.0/initial_catalog_size*100
        scan_progress_message(True, fix, current_cycle, catalog_name, number_of_issues, total_number_of_issues, percentage, chunk_number, log)

    if number_of_issues > 0:
        # print 'total_number_of_issues: {0}'.format(total_number_of_issues)
        return True, number_of_issues
    return False


def build_catalog_dict(dmd, log):
    """Builds a list of catalogs present and > 0 objects"""

    catalogs_to_check = {
        'Networks.ipSearch': 'dmd.Networks.ipSearch',
        'IPv6Networks.ipSearch': 'dmd.IPv6Networks.ipSearch',
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
                                     description="Removes old unused ip addresses. Documentation available at "
                                         "https://support.zenoss.com/hc/en-us/articles/203263699")

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to remove any stale references")
    parser.add_argument("-n", "--cycles", action="store", default="12", type=int,
                        help="maximum times to cycle (with --fix)")
    parser.add_argument("-l", "--list", action="store_true", default=False,
                        help="output all supported catalogs")
    parser.add_argument("-c", "--catalog", action="store", default="",
                        help="only scan/fix specified catalog")

    return vars(parser.parse_args())


def main():
    '''Removes old unused ip addresses.  If --fix, attempts to remove old unused ip addresses.
       Builds list of available non-empty catalogs.'''

    execution_start = time.time()
    cli_options = parse_options()
    log = configure_logging('zennetworkclean')
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not get_lock("zenoss.toolbox", log):
        sys.exit(1)

    # Obtain dmd ZenScriptBase connection
    dmd = ZenScriptBase(noopts=True, connect=True).dmd
    log.debug("ZenScriptBase connection obtained")

    any_issue = [False, 0]
    unrecognized_catalog = False

    # Build list of catalogs, then process catalog(s) and perform reindex if --fix
    present_catalog_dict = build_catalog_dict(dmd, log)
    if cli_options['list']:
    # Output list of present catalogs to the UI, perform no further operations
        print "List of supported Zenoss catalogs to examine:\n"
        print "\n".join(present_catalog_dict.keys())
        log.info("zennetworkclean finished - list of supported catalogs output to CLI")
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
    log.info("zennetworkclean completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")

    if any_issue and not cli_options['fix']:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()

