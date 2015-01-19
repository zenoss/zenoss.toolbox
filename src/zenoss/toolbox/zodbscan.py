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
import cStringIO
import tempfile
import cPickle
import ZConfig

from pickle import Unpickler as UnpicklerBase
from collections import deque
from time import localtime, strftime
from multiprocessing import Lock, Value
from relstorage.zodbpack import schema_xml
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from Products.ZenUtils.AutoGCObjectReader import gc_cache_every
from Products.ZenUtils.GlobalConfig import getGlobalConfiguration
from Products.ZenRelations.ToManyContRelationship import ToManyContRelationship
from Products.ZenRelations.RelationshipBase import RelationshipBase
from ZODB.transact import transact
from ZODB.POSException import POSKeyError
from ZODB.DB import DB
from ZODB.utils import u64


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


schema = ZConfig.loadSchemaFile(cStringIO.StringIO(schema_xml))


class Analyzer(UnpicklerBase):
    """ Able to analyze an object's pickle to try to figure out the name/class of the problem oid.  """
    def __init__(self, pickle, problem_oid):
        UnpicklerBase.__init__(self, cStringIO.StringIO(pickle))
        self.problem_oid = problem_oid
        self._marker = object()
        self.klass = None

    def persistent_load(self, pickle_id):
        if isinstance(pickle_id, tuple):
            oid, klass = pickle_id
            if oid == self.problem_oid:
                self.klass = klass
                return self._marker
        else:
            pass


def get_refs(p):
    """ Generator-using version of ZODB.serialize.references """
    refs = []
    u = cPickle.Unpickler(cStringIO.StringIO(p))
    u.persistent_load = refs
    u.noload()
    u.noload()
    for ref in refs:
        if isinstance(ref, tuple):
            yield ref[0]
        elif isinstance(ref, str):
            yield ref
        else:
            assert isinstance(ref, list)
            yield ref[1][:2]


def get_config(database=None):
    conf = getGlobalConfiguration()

    if database:
        conf['zodb-db'] = conf['mysqldb'] = database
    else:
        conf['mysqldb'] = conf.get('mysqldb', conf.get('zodb-db'))
        conf['zodb-db'] = conf.get('zodb-db', conf.get('mysqldb'))

    zodb_socket = conf.get('mysqlsocket', conf.get('zodb-socket'))

    if zodb_socket:
        conf['socket'] = 'unix_socket %s' % zodb_socket
    else:
        conf['socket'] = ''

    newer_conf = {
        'zodb-host': conf.get('host'),
        'zodb-port': conf.get('port'),
        'zodb-db': conf.get('mysqldb'),
        'zodb-user': conf.get('mysqluser'),
        'zodb-password': conf.get('mysqlpasswd')
    }

    newer_conf.update(conf)

    _storage_config = """
        <relstorage>
            pack-gc true
            keep-history false
            <mysql>
                host %(zodb-host)s
                port %(zodb-port)s
                db %(zodb-db)s
                user %(zodb-user)s
                passwd %(zodb-password)s
                %(socket)s
            </mysql>
        </relstorage>
        """ % newer_conf

    with tempfile.NamedTemporaryFile() as configfile:
        configfile.write(_storage_config)
        configfile.flush()
        config, handler = ZConfig.loadConfig(schema, configfile.name)
        return config


class PKEReporter(object):
    def __init__(self, db='zodb'):
        self._dbname = db
        self._config = get_config(db)
        self._storage = self._config.storages[0].open()
        self._db = DB(self._storage)
        self._conn = self._db.open()
        self._app = self._conn.root()
        self._size = self.get_total_count()

    def get_total_count(self):
        connmanager = self._storage._adapter.connmanager
        conn, cursor = connmanager.open()
        try:
            cursor.execute("SELECT count(zoid) from object_state")
            row = cursor.fetchone()
            return long(row[0])
        finally:
            connmanager.close(conn, cursor)

    def analyze(self, parent_oid, child_oid):
        parent_state = self._storage.load(parent_oid)[0]
        pickler = Analyzer(parent_state, child_oid)
        pickler.load()
        result = pickler.load()
        name = None
        # First try to get the name from the pickle state
        try:
            for k, v in result.iteritems():
                if v is pickler._marker:
                    name = k
                    break
        except Exception:
            pass
        if not name:
            # Now load up the child and see if it has an id
            child = self._conn[child_oid]
            try:
                name = child.id
            except Exception:
                try:
                    name = child.getId()
                except Exception:
                    pass
        if not name:
            # Check the actual attributes on the parent
            parent = self._conn[parent_oid]
            try:
                for k, v in parent.__dict__.iteritems():
                    try:
                        if v == child:
                            name = k
                            break
                    except Exception:
                        pass
            except AttributeError:  # catch these errors -  AttributeError: 'BTrees.OIBTree.OIBTree' object has no attribute '__dict__'
                pass
        return name, pickler.klass

    @staticmethod
    def oid_versions(oid):
        u64ed = u64(oid)
        oid_0xstyle = "0x%08x" % u64ed
        repred = repr(oid)
        return u64ed, oid_0xstyle, repred

    def report(self, oid, ancestors, log):
        parent_oid = ancestors[-2]
        parent_klass = None
        try:
            immediate_parent = self._conn[parent_oid]
            parent_klass = immediate_parent.__class__
            path = immediate_parent.getPrimaryPath()
        except Exception:
            # Not a PrimaryPathObjectManager, do it manually
            path = ['']
            for (a, b) in zip(ancestors[:-2], ancestors[1:-1]):
                name, klass = self.analyze(a, b)
                path.append(name)
            parent_klass = klass
        path = filter(None, path)
        name, klass = self.analyze(*ancestors[-2:])
        par_u64, par_0x, par_rep = self.oid_versions(parent_oid)
        oid_u64, oid_0x, oid_rep = self.oid_versions(oid)
        log.critical(""" DANGLING REFERENCE (POSKeyError) FOUND:
PATH: {path}
TYPE: {type}
OID:  {par_0x} {par_rep} {par_u64}
Refers to a missing object:
    NAME: {name}
    TYPE: {klass}
    OID:  {oid_0x} {oid_rep} {oid_u64} """.format(path='/'.join(path),
           type=parent_klass, name=name, klass=klass,
           par_u64=par_u64, par_0x=par_0x, par_rep=par_rep,
           oid_u64=oid_u64, oid_0x=oid_0x, oid_rep=oid_rep))

    def verify(self, root, log, number_of_issues):

        database_size = self._size
        scanned_count = 0
        progress_bar_chunk_size = 1

        if (database_size > 50):
            progress_bar_chunk_size = (database_size//50) + 1

        inline_print("[%s]  Scanning  [%-50s] %3d%% " % (time.strftime("%Y-%m-%d %H:%M:%S"), '='*0, 0))

        seen = set()
        path = ()
        stack = deque([(root, path)])
        curstack, stack = stack, deque([])
        while curstack or stack:
            oid, path = curstack.pop()
            scanned_count = len(seen)

            if (scanned_count % progress_bar_chunk_size) == 0:
                chunk_number = scanned_count // progress_bar_chunk_size
                if number_of_issues.value() > 2:
                    inline_print("[%s]  CRITICAL  [%-50s] %3d%% [%d Dangling References]" %
                                 (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk_number, 2*chunk_number, number_of_issues.value()))
                elif number_of_issues.value() == 1:
                    inline_print("[%s]  CRITICAL  [%-50s] %3d%% [%d Dangling Reference]" %
                                 (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk_number, 2*chunk_number, number_of_issues.value()))
                else:
                    inline_print("[%s]  Scanning  [%-50s] %3d%% " % (time.strftime("%Y-%m-%d %H:%M:%S"), '='*chunk_number, 2*chunk_number))

            if (oid not in seen):
                try:
                    state = self._storage.load(oid)[0]
                    seen.add(oid)
                except POSKeyError:
                    self.report(oid, path, log)
                    number_of_issues.increment()
                else:
                    refs = get_refs(state)
                    stack.extend((o, path + (o,)) for o in set(refs) - seen)

            if not curstack:
                curstack = stack
                stack = deque([])

        if number_of_issues.value() > 0:
            inline_print("[%s]  CRITICAL  [%-50s] %3.0d%% [%d Dangling References]\n" %
                         (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100, number_of_issues.value()))
        else:
            inline_print("[%s]  Verified  [%-50s] %3.0d%%\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), '='*50, 100))

        return number_of_issues, len(seen), self._size

    def run(self, log, number_of_issues):
        print("[%s] Examining %d items in the '%s' database:" %
              (strftime("%Y-%m-%d %H:%M:%S", localtime()), self._size,  self._dbname))
        log.info("Examining %d items in %s database" % (self._size, self._dbname))

        oid = '\x00\x00\x00\x00\x00\x00\x00\x01'

        with gc_cache_every(1000, self._db):
            reported, scanned, total = self.verify(oid, log, number_of_issues)

        if (100.0*scanned/total) < 90.0:
            print("  ** %3.2f%% of %s objects not reachable - examine your zenossdbpack settings **" %
                  ((100.0-100.0*scanned/total), self._dbname))
            log.info("%3.2f%% of %s objects not reachable - examine your zenossdbpack settings" %
                     ((100.0-100.0*scanned/total), self._dbname))
        print


def parse_options():
    """Defines command-line options for script """
    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans zodb for dangling references. Additional documentation at "
                                                  "https://support.zenoss.com/hc/en-us/articles/203118175")

    parser.add_argument("-v10", "--debug", action="store_true", default=False,
                        help="verbose log output (debug logging)")
    return vars(parser.parse_args())


def main():
    """Scans through zodb hierarchy checking objects for dangling references"""

    execution_start = time.time()
    sys.path.append ("/opt/zenoss/Products/ZenModel")               # From ZEN-12160

    cli_options = parse_options()
    log = configure_logging('zodbscan')
    log.info("Command line options: %s" % (cli_options))
    if cli_options['debug']:
        log.setLevel(logging.DEBUG)

    #logging.getLogger('relstorage').setLevel(logging.CRITICAL)
    #logging.getLogger('ZODB.Connection').setLevel(logging.CRITICAL)
        
    # Attempt to get the zenoss.toolbox lock before any actions performed
    if not get_lock("zenoss.toolbox", log):
        sys.exit(1)

    number_of_issues = Counter(0)

    PKEReporter('zodb').run(log, number_of_issues)
    log.info("%d Dangling References were detected" % (number_of_issues.value()))

    print("[%s] Execution finished in %s\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()),
                                               datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zodbscan completed in %1.2f seconds" % (time.time() - execution_start))
    log.info("############################################################")

    if (number_of_issues.value() > 0):
        print("** WARNING ** Dangling Reference(s) were detected - Consult KB article at")
        print("      https://support.zenoss.com/hc/en-us/articles/203118175\n")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
