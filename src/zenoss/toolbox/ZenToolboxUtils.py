##############################################################################
#
# Copyright (C) Zenoss, Inc. 2016, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import argparse
import logging
import os
import socket
import sys
import time

from multiprocessing import Lock, Value


def configure_logging(name, version, tmpdir):
    '''Returns a python logging object for zenoss.toolbox tool usage'''

    # Confirm %tmpdir, $ZENHOME and check for $ZENHOME/log/toolbox (create if missing)
    if not os.path.exists(tmpdir):
        print "%s doesn't exist - aborting" % (tmpdir)
        exit(1)
    zenhome_path = os.getenv("ZENHOME")
    if not zenhome_path:
        print "$ZENHOME undefined - are you running as the zenoss user?"
        exit(1)
    log_file_path = os.path.join(zenhome_path, 'log', 'toolbox')
    if not os.path.exists(log_file_path):
        os.makedirs(log_file_path)

    # Setup "trash" toolbox log file (needed for ZenScriptBase log overriding)
    logging.basicConfig(filename=os.path.join(tmpdir,'toolbox.log.tmp'), filemode='w', level=logging.INFO)

    # Create full path filename string for logfile, create RotatingFileHandler
    toolbox_log = logging.getLogger("%s" % (name))
    toolbox_log.setLevel(logging.INFO)
    log_file_name = os.path.join(zenhome_path, 'log', 'toolbox', '%s.log' % (name))
    handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=8192*1024, backupCount=5)

    # Set logging.Formatter for format and datefmt, attach handler
    formatter = logging.Formatter('%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s', '%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    toolbox_log.addHandler(handler)

    # Print initialization string to console, log status to logfile
    toolbox_log.info("############################################################")
    toolbox_log.info("Initializing %s (version %s)", name, version)

    return toolbox_log, log_file_name


def get_lock(lock_name, log):
    '''Global lock function to keep multiple tools from running at once'''
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + lock_name)
        log.debug("Acquired '%s' execution lock", lock_name)
    except socket.error:
        print("[%s] Aborting - unable to acquire %s socket lock - are other tools running?\n" %
              (time.strftime("%Y-%m-%d %H:%M:%S"), lock_name))
        log.error("'%s' lock already exists - unable to acquire - exiting", lock_name)
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

    def reset(self):
        with self.lock:
            self.val.value = 0


def parse_options(scriptVersion, description_string):
    """Defines command-line options for script """
    parser = argparse.ArgumentParser(version=scriptVersion, description=description_string)

    calculatedTmpDir = next((os.getenv(n) for n in ("TMP", "TEMP", "TMPDIR") if n in os.environ), None)
    if not calculatedTmpDir:
        calculatedTmpDir = "/tmp"

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    parser.add_argument("--tmpdir", action="store", default=calculatedTmpDir,
                            help="override the TMPDIR setting")

    return parser


def send_summary_event(eventSummaryMsg, eventSeverity, eventKey, eventClassKey, dedupid, docURL, dmd):
    """ Sends an event from a tool (with an established dmd connection) to Zenoss """

    dmd.ZenEventManager.sendEvent({
        'device'        : 'localhost',
        'summary'       : eventSummaryMsg,
        'message'       : eventSummaryMsg,
        'component'     : 'zenoss_toolbox',
        'severity'      : eventSeverity,
        'eventClass'    : '/Status',
        'eventKey'      : eventKey,
        'dedupid'       : dedupid,
        'eventClassKey' : eventClassKey,
        'details'       : docURL
    })
