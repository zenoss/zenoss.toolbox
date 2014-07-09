#!/opt/zenoss/bin/python
########################

scriptVersion = "0.9.0"

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


execution_start = time.time()
sys.path.append ("/opt/zenoss/Products/ZenModel")		# From ZEN-12160
number_of_issues = 0
log_file_path = os.path.join(os.getenv("ZENHOME"), 'log', 'toolbox')
if not os.path.exists(log_file_path):
    os.makedirs(log_file_path)
log_file_name = os.path.join(os.getenv("ZENHOME"), 'log', 'toolbox', 'zodbscan.log')
logging.basicConfig(filename='%s' % (log_file_name),
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(levelname)s %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
log = logging.getLogger("zen.zodbscan")
print("\n[%s] Initializing zodbscan (detailed log at %s)\n" %
      (strftime("%Y-%m-%d %H:%M:%S", localtime()), log_file_name))
log.setLevel(logging.INFO)
log.info("Initializing zodbscan")

logging.getLogger('relstorage').setLevel(logging.CRITICAL)
logging.getLogger('ZODB.Connection').setLevel(logging.CRITICAL)


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

    def report(self, oid, ancestors):
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

    def verify(self, root):
        global number_of_issues

        database_size = self._size
        scanned_count = 0
        progress_bar_chunk_size = 1
        number_of_issues = 0

        if (database_size > 50):
            progress_bar_chunk_size = (database_size//50) + 1

        progress_bar("\r  Scanning  [%-50s] %3d%% " % ('='*0, 0))

        seen = set()
        path = ()
        stack = deque([(root, path)])
        curstack, stack = stack, deque([])
        while curstack or stack:
            oid, path = curstack.pop()
            scanned_count = len(seen)

            if (scanned_count % progress_bar_chunk_size) == 0:
                chunk_number = scanned_count // progress_bar_chunk_size
                if number_of_issues > 2:
                    progress_bar("\r  CRITICAL  [%-50s] %3d%% [%d Dangling References]" %
                                 ('='*chunk_number, 2*chunk_number, number_of_issues))
                elif number_of_issues == 1:
                    progress_bar("\r  CRITICAL  [%-50s] %3d%% [%d Dangling Reference]" %
                                 ('='*chunk_number, 2*chunk_number, number_of_issues))
                else:
                    progress_bar("\r  Scanning  [%-50s] %3d%% " % ('='*chunk_number, 2*chunk_number))

            if (oid not in seen):
                try:
                    state = self._storage.load(oid)[0]
                    seen.add(oid)
                except POSKeyError:
                    self.report(oid, path)
                    number_of_issues += 1
                else:
                    refs = get_refs(state)
                    stack.extend((o, path + (o,)) for o in set(refs) - seen)

            if not curstack:
                curstack = stack
                stack = None
                stack = deque([])

        if number_of_issues > 0:
            progress_bar("\r  CRITICAL  [%-50s] %3.0d%% [%d Dangling References]\n" %
                         ('='*50, 100, number_of_issues))
        else:
            progress_bar("\r  Verified  [%-50s] %3.0d%%\n" % ('='*50, 100))

        return number_of_issues, len(seen), self._size

    def run(self):
        print("[%s] Examining %d items in the '%s' database:" %
              (strftime("%Y-%m-%d %H:%M:%S", localtime()), self._size,  self._dbname))
        log.info("Examining %d items in %s database" % (self._size, self._dbname))

        oid = '\x00\x00\x00\x00\x00\x00\x00\x01'

        with gc_cache_every(1000, self._db):
            reported, scanned, total = self.verify(oid)

        if (100.0*scanned/total) < 90.0:
            print("  ** %3.2f%% of %s objects not reachable - examine your zenossdbpack settings **" %
                  ((100.0-100.0*scanned/total), self._dbname))
            log.info("%3.2f%% of %s objects not reachable - examine your zenossdbpack settings" %
                     ((100.0-100.0*scanned/total), self._dbname))
        print


def parse_options():
    """Defines command-line options for script """
    parser = argparse.ArgumentParser(version=scriptVersion,
                                     description="Scans zodb/zodb_session for dangling references")
    return vars(parser.parse_args())


def main():
    """Scans through zodb hierarchy checking objects for dangling references"""

    # Attempt to get the zenoss-toolbox lock before any actions performed
    if not get_lock("zenoss-toolbox"):
        sys.exit(1)

    global number_of_issues

    cli_options = parse_options()

    PKEReporter('zodb').run()

    print("[%s] Execution finished in %s\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()),
                                               datetime.timedelta(seconds=int(time.time() - execution_start))))
    log.info("zodbscan completed in %1.2f seconds" % (time.time() - execution_start))

    if number_of_issues == 0:
        sys.exit(0)
    if number_of_issues == 1:
        print("A Dangling Reference (POSKeyError) was detected:")
    else:
        print("Dangling References (POSKeyErrors) were detected:")

    print("  * Check detailed log file at %s" % (log_file_name))
    print("  * Consult http://support.zenoss.com/ics/support/KBAnswer.asp?questionID=217\n")
    sys.exit(1)


if __name__ == "__main__":
    main()
