##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

scriptVersion = "1.6.2"

import abc
import argparse
import datetime
import Globals
import logging
import os
import re
import socket
import sys
import time
import traceback
import transaction

from multiprocessing import Lock, Value
from time import localtime, strftime
from ZODB.POSException import POSKeyError
from ZODB.utils import u64
from Products.ZenRelations.ToManyContRelationship import ToManyContRelationship
from Products.ZenRelations.RelationshipBase import RelationshipBase
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.ZenUtils.Utils import unused
try:
    from ZenPacks.zenoss.AdvancedSearch.SearchManager import SearchManager, SEARCH_MANAGER_ID
except ImportError:
    pass


unused(Globals) 


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


class Fixer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def fixable(self, ex, objId, parentPath, dmd, log):
        """
        Return a no-argument callable object that will perform the fix
        when invoked or None if not fixable.
        """


class RelFixer(Fixer):
    def fixable(self, ex, relId, parentPath, dmd, log):
        """
        Return True if this object can fix the exception.
        """
        try:
            parent = dmd.getObjByPath(parentPath)
            relationship = parent._getOb(relId)
            if not isinstance(relationship, RelationshipBase):
                return None
            badobj = getattr(relationship, "_objects", None)
            if badobj is None:
                log.warning("Cannot fix relationship - no _objects attribute")
                return None
            exOID = getOID(ex)
            relOID = getPOID(relationship._objects)
            if exOID == relOID:
                return lambda: self._fix(exOID, relOID, relationship, parent, dmd, log)
            else:
                log.warning("Cannot fix this relationship - exOID %s != relOID %s" % (exOID, relOID))
        except:
            return None

    def _fix(self, exOID, relOID, relationship, parent, dmd, log):
        """ Attempt to fix the POSKeyError """
        cls = relationship._objects.__class__
        relationship._objects = cls()
        parent._p_changed = True
        transaction.commit()


class SearchManagerFixer(Fixer):
    """
    SearchManagerFixer fixes SearchManager POSKeyErrors like:
        POSKeyError: 0x0683923b on attribute 'SearchManager' of app.zport.dmd.ZenUsers.svs
    """
    # >>> dmd.ZenUsers.svs.SearchManager.__class__
    # <class 'ZenPacks.zenoss.AdvancedSearch.SearchManager.SearchManager'>
    # >>> find('svs')
    # <UserSettings at /zport/dmd/ZenUsers/svs>
    # >>> d=_
    # >>> d._delOb('SearchManager')
    # >>> commit()
    def fixable(self, ex, objId, parentPath, dmd, log):
        """ Return True if this object can fix the exception.  """
        if objId != 'SearchManager':
            return None

        parent = dmd.getObjByPath(parentPath)
        obj = parent._getOb(objId)
        if not isinstance(obj, SearchManager):
            return None
        exOID = getOID(ex)
        relOID = getPOID(obj)
        if exOID == relOID:
            return lambda: self._fix(exOID, parent, dmd, log)

        return None

    def _fix(self, exOID, parent, dmd, log):
        """ Delete only; a new one will be created when a SearchProvider is requested.  """
        try:
            parent._delOb('SearchManager')
        except Exception as e:
            log.exception(e)
        transaction.commit()

        try:
            parent._setObject(SEARCH_MANAGER_ID, SearchManager(SEARCH_MANAGER_ID))
        except Exception as e:
            log.exception(e)
        transaction.commit()


class ComponentSearchFixer(Fixer):
    """
    ComponentSearchFixer fixes ComponentSearch POSKeyErrors like:
        POSKeyError: 0x070039e0 on attribute 'componentSearch' of
          app.zport.dmd.Devices.Network.Juniper.mx.mx_240.devices.edge1.fra
    """

    def fixable(self, ex, objId, parentPath, dmd, log):
        """ Return True if this object can fix the exception.  """
        if objId != 'componentSearch':
            return None

        parent = dmd.getObjByPath(parentPath)
        obj = parent._getOb(objId)
        exOID = getOID(ex)
        relOID = getPOID(obj)
        if exOID == relOID:
            return lambda: self._fix(exOID, parent, dmd, log)

        return None

    def _fix(self, exOID, parent, dmd, log):
        """ Attempt to remove and recreate the componentSearch() """
        try:
            parent._delOb('componentSearch')
        except Exception as e:
            log.exception(e)
        transaction.commit()

        try:
            parent._create_componentSearch()
        except Exception as e:
            log.exception(e)
        transaction.commit()


_fixits = [RelFixer(), SearchManagerFixer(), ComponentSearchFixer(), ]


def _getEdges(node):
    cls = node.aq_base
    names = set(node.objectIds() if hasattr(cls, "objectIds") else [])
    relationships = set(
        node.getRelationshipNames()
        if hasattr(cls, "getRelationshipNames") else []
    )
    return (names - relationships), relationships


_RELEVANT_EXCEPTIONS = (POSKeyError, KeyError, AttributeError)


def _getPathStr(path):
    return "app%s" % ('.'.join(path)) if len(path) > 1 else "app"


def fixPOSKeyError(exname, ex, objType, objId, parentPath, dmd, log, counters):
    """
    Fixes POSKeyErrors given:
        Name of exception type object,
        Exception,
        Type of problem object,
        Name (ID) of the object,
        The path to the parent of the named object
    """
    # -- verify that the OIDs match
    for fixer in _fixits:
        fix = fixer.fixable(ex, objId, parentPath, dmd, log)
        if fix:
            log.info("Attempting to repair %s issue on %s" % (ex, objId))
            counters['repair_count'].increment()
            fix()
            break


def getPOID(obj):
    # from ZODB.utils import u64
    return "0x%08x" % u64(obj._p_oid)


def getOID(ex):
    return "0x%08x" % int(str(ex), 16)


def findPOSKeyErrors(topnode, attempt_fix, use_unlimited_memory, dmd, log, counters):
    """ Processes issues as they are found, handles progress output, logs to output file """

    PROGRESS_INTERVAL = 829  # Prime number near 1000 ending in a 9, used for progress bar

    # Objects that will have their children traversed are stored in 'nodes'
    nodes = [topnode]
    while nodes:
        node = nodes.pop(0)
        counters['item_count'].increment()
        path = node.getPhysicalPath()
        path_string = "/".join(path)

        if (counters['item_count'].value() % PROGRESS_INTERVAL) == 0:
            if not use_unlimited_memory:
                transaction.abort()
            progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                         counters['repair_count'].value(), attempt_fix)

        try:
            attributes, relationships = _getEdges(node)
        except _RELEVANT_EXCEPTIONS as e:
            log.warning("%s: %s %s '%s'" %
                        (type(e).__name__, e, "while retreiving children of", path_string))
            counters['error_count'].increment()
            if attempt_fix:
                if isinstance(e, POSKeyError):
                    fixPOSKeyError(type(e).__name__, e, "node", name, path, dmd, log, counters)
            continue
        except Exception as e:
            log.exception(e)

        for name in relationships:
            try:
                if (counters['item_count'].value() % PROGRESS_INTERVAL) == 0:
                    if not use_unlimited_memory:
                        transaction.abort()
                    progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                                 counters['repair_count'].value(), attempt_fix)
                counters['item_count'].increment()

                rel = node._getOb(name)
                rel()
                # ToManyContRelationship objects should have all referenced objects traversed
                if isinstance(rel, ToManyContRelationship):
                    nodes.append(rel)
            except SystemError as e:
                # to troubleshoot traceback in:
                #   https://dev.zenoss.com/tracint/pastebin/4769
                # ./findposkeyerror --fixrels /zport/dmd/
                #   SystemError: new style getargs format but argument is not a tuple
                log.warning("%s: %s on %s '%s' of %s" %
                            (type(e).__name__, e, "relationship", name, path_string))
                raise  # Not sure why we are raising this vs. logging and continuing
            except _RELEVANT_EXCEPTIONS as e:
                counters['error_count'].increment()
                log.warning("%s: %s on %s '%s' of %s" %
                            (type(e).__name__, e, "relationship", name, path_string))
                if attempt_fix:
                    if isinstance(e, POSKeyError):
                        fixPOSKeyError(type(e).__name__, e, "attribute", name, path, dmd, log, counters)
            except Exception as e:
                log.warning("%s: %s on %s '%s' of %s" %
                            (type(e).__name__, e, "relationship", name, path_string))

        for name in attributes:
            try:
                if (counters['item_count'].value() % PROGRESS_INTERVAL) == 0:
                    if not use_unlimited_memory:
                        transaction.abort()
                    progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                                 counters['repair_count'].value(), attempt_fix)
                counters['item_count'].increment()
                childnode = node._getOb(name)
                childnode.getId()
            except _RELEVANT_EXCEPTIONS as e:
                counters['error_count'].increment()
                log.warning("%s: %s on %s '%s' of %s" %
                            (type(e).__name__, e, "attribute", name, path_string))
                if attempt_fix:
                    if isinstance(e, POSKeyError):
                        fixPOSKeyError(type(e).__name__, e, "attribute", name, path, dmd, log, counters)
            except Exception as e:
                log.warning("%s: %s on %s '%s' of %s" %
                            (type(e).__name__, e, "relationship", name, path_string))
            else:
                # No exception, so it should be safe to add this child node as a traversable object.
                nodes.append(childnode)

    if not use_unlimited_memory:
        transaction.abort()
    progress_bar(counters['item_count'].value(), counters['error_count'].value(),
                 counters['repair_count'].value(), attempt_fix)


def parse_options():
    """Defines command-line options for script """
    """ NOTE: With --unlimitedram in my testing, I have seen RAM usage grow to just over 2x the size of
    'du -h /opt/zends/data/zodb'.  For a 20GB /opt/zends/data/zodb folder, I saw RAM usage of ~ 42GB"""

    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans a zodb path for POSKeyErrors - addtional information "
                                                 "at https://support.zenoss.com/hc/en-us/articles/203117795")

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    parser.add_argument("-f", "--fix", action="store_true", default=False,
                        help="attempt to fix ZenRelationship objects")
    parser.add_argument("-p", "--path", action="store", default="/", type=str,
                        help="base path to scan from (Devices.Server)?")
    parser.add_argument("-u", "--unlimitedram", action="store_true", default=False,
                        help="skip transaction.abort() - unbounded RAM, ~40%% faster")

    return vars(parser.parse_args())


def main():
    """ Scans through zodb hierarchy (from user-supplied path, defaults to /,  checking for PKEs """

    execution_start = time.time()
    cli_options = parse_options()
    log = configure_logging('findposkeyerror')
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)
        
    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not get_lock("zenoss.toolbox", log):
        sys.exit(1)
        
    # Obtain dmd ZenScriptBase connection
    dmd = ZenScriptBase(noopts=True, connect=True).dmd
    log.debug("ZenScriptBase connection obtained")

    counters = {
        'item_count': Counter(0),
        'error_count': Counter(0),
        'repair_count': Counter(0)
        }

    processed_path = re.split("[./]", cli_options['path'])
    if processed_path[0] == "app":
        processed_path = processed_path[1:]
    processed_path = '/'.join(processed_path) if processed_path else '/'

    try:
        folder = dmd.getObjByPath(processed_path)
    except KeyError:
        print "Invalid path: %s" % (cli_options['path'])
    else:
        print("[%s] Examining items under the '%s' path (%s):\n" %
              (strftime("%Y-%m-%d %H:%M:%S", localtime()), cli_options['path'], folder))
        log.info("Examining items under the '%s' path (%s)" % (cli_options['path'], folder))
        findPOSKeyErrors(folder, cli_options['fix'], cli_options['unlimitedram'], dmd, log, counters)
        print

    print("\n[%s] Execution finished in %s\n" %
          (strftime("%Y-%m-%d %H:%M:%S", localtime()),
           datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("findposkeyerror completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")

    if ((counters['error_count'].value() > 0) and not cli_options['fix']):
        print("** WARNING ** Issues were detected - Consult KB article at")
        print("      https://support.zenoss.com/hc/en-us/articles/203117795\n")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
