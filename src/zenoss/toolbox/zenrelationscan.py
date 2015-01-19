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

from multiprocessing import Lock, Value
from time import localtime, strftime
from Products.CMFCore.utils import getToolByName
from Products.ZenUtils.Utils import getAllConfmonObjects
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.Zuul.catalog.events import IndexingEvent
from ZODB.transact import transact
from zope.event import notify

from ZODB.POSException import POSKeyError



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


class Counter(object):
    def __init__(self, initval=0):
        self.val = Value('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value


def progress_bar(items, errors, repairs, fix_value):
    if fix_value:
        inline_print("[%s]  | Items Scanned: %12d | Errors:  %6d | Repairs: %6d |  " %
                     (time.strftime("%Y-%m-%d %H:%M:%S"), items, errors, repairs))
    else:
        inline_print("[%s]  | Items Scanned: %12d | Errors:  %6d |  " % (time.strftime("%Y-%m-%d %H:%M:%S"), items, errors))


def scan_relationships(attempt_fix, max_cycles, use_unlimited_memory, dmd, log, counters):
    '''Scan through zodb relationships looking for broken references'''

#    ENTIRETY OF REBUILD CODE FROM ZenUtils/CheckRelations.py (for reference)
#    def rebuild(self):
#        repair = self.options.repair
#        ccount = 0
#        for object in getAllConfmonObjects(self.dmd):
#            ccount += 1
#            self.log.debug("checking relations on object %s"
#                                % object.getPrimaryDmdId())
#            object.checkRelations(repair=repair)
#            ch = object._p_changed
#            if not ch: object._p_deactivate()
#            if ccount >= self.options.savepoint:
#                transaction.savepoint()
#                ccount = 0
#        if self.options.nocommit:
#            self.log.info("not commiting any changes")
#        else:
#            trans = transaction.get()
#            trans.note('CheckRelations cleaned relations' )
#            trans.commit()

    PROGRESS_INTERVAL = 829  # Prime number near 1000 ending in a 9, used for progress bar

    print("[%s] Examining ZenRelations...\n" % (time.strftime("%Y-%m-%d %H:%M:%S")))
    log.info("Examining ZenRelations...")

    number_of_issues = -1
    current_cycle = 0
    if not attempt_fix:
        max_cycles = 1

    progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                         counters['repair_count'].value(), attempt_fix)

    while ((current_cycle < max_cycles) and (number_of_issues != 0)):
        number_of_issues = 0
        current_cycle += 1
        if (attempt_fix):
            log.info("Beginning cycle %d" % (current_cycle))

        try:
            relationships_to_check = getAllConfmonObjects(dmd)
        except Exception:
            raise

        while True:
            try:
                object = relationships_to_check.next()
                counters['item_count'].increment()

                if (counters['item_count'].value() % PROGRESS_INTERVAL) == 0:
                    if not use_unlimited_memory:
                        transaction.abort()
                    progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                                 counters['repair_count'].value(), attempt_fix)
                    log.debug("Processed %d items" % (counters['item_count'].value()))

                try:
                    object.checkRelations(repair=attempt_fix)
                    changed = object._p_changed
                    if not changed:
                        object._p_deactivate()
                    else:
                        transaction.commit()
                    log.debug("Checked object %s" % (object.getPrimaryDmdId()))
                except Exception as e:
                    log.exception(e)
                    counters['error_count'].increment()
                    counters['repair_count'].increment()
                except:
                    try:
                        log.error("Object %s had broken relationship" % (object.getPrimaryDmdId()))
                    except:
                        log.error("Object had issues loading - PKE")
                    counters['error_count'].increment()
                    counters['repair_count'].increment()

            except StopIteration:
                break
            except Exception as e:
                log.exception(e)
                if not use_unlimited_memory:
                    transaction.abort()
                progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                             counters['repair_count'].value(), attempt_fix)
                print("\n\n#################################################################")
                print "CRITICAL: Exception encountered - aborting.  Please see log file."
                print("#################################################################")
                return

    if not use_unlimited_memory:
        transaction.abort()
    progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                 counters['repair_count'].value(), attempt_fix)
    print


def parse_options():
    """Defines and parses command-line options for script """
    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans ZenRelations for issues. Additional documentat at "
                                                  "https://support.zenoss.com/hc/en-us/articles/203121165")

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to remove any invalid references")
    parser.add_argument("-n", "--cycles", action="store", default="2", type=int,
                        help="maximum times to cycle (with --fix)")
    parser.add_argument("-u", "--unlimitedram", action="store_true", default=False,
                        help="skip transaction.abort() - unbounded RAM, ~40%% faster")

    return vars(parser.parse_args())


def main():
    '''Scans zodb objects for ZenRelations issues.  If --fix, attempts repair.'''

    execution_start = time.time()
    cli_options = parse_options()
    log = configure_logging('zenrelationscan')
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    counters = {
        'item_count': Counter(0),
        'error_count': Counter(0),
        'repair_count': Counter(0)
        }

    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not get_lock("zenoss.toolbox", log):
        sys.exit(1)

    # Obtain dmd ZenScriptBase connection
    dmd = ZenScriptBase(noopts=True, connect=True).dmd
    log.debug("ZenScriptBase connection obtained")

    scan_relationships(cli_options['fix'], cli_options['cycles'], cli_options['unlimitedram'], dmd, log, counters)

    # Print final status summary, update log file with termination block
    print("\n[%s] Execution finished in %s\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()),
           datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zenrelationscan completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")

    if ((counters['error_count'].value() > 0) and not cli_options['fix']):
        print("** WARNING ** Issues were detected - Consult KB article at")
        print("      https://support.zenoss.com/hc/en-us/articles/203121165\n")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
