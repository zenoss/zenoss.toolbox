#!/usr/bin/env python
import sys
import logging
import cStringIO
import tempfile
import cPickle
from pickle import Unpickler as UnpicklerBase
from collections import deque

import Globals
import ZConfig
from relstorage.zodbpack import schema_xml
from ZODB.POSException import POSKeyError
from ZODB.DB import DB
from ZODB.utils import u64

from Products.ZenUtils.GlobalConfig import getGlobalConfiguration


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pkereport")

logging.getLogger('relstorage').setLevel(logging.CRITICAL)


schema = ZConfig.loadSchemaFile(cStringIO.StringIO(schema_xml))


class Analyzer(UnpicklerBase):
    """
    Able to analyze an object's pickle to try to figure out the name/class of
    the problem oid.
    """
    def __init__(self, pickle, problem_oid):
        UnpicklerBase.__init__(self, cStringIO.StringIO(pickle))
        self.problem_oid = problem_oid
        self._marker = object()
        self.klass = None

    def persistent_load(self, (oid, klass)):
        if oid == self.problem_oid:
            self.klass = klass
            return self._marker


def get_refs(p):
    """
    Generator-using version of ZODB.serialize.referencesf
    """
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
    _global_conf = getGlobalConfiguration()
    if database:
        _global_conf['mysqldb'] = database
    zodb_socket = _global_conf.get('mysqlsocket')
    if zodb_socket:
        _global_conf['socket-option'] = 'unix_socket %s' % zodb_socket
    else:
        _global_conf['socket-option'] = ''

    _storage_config = """
        <relstorage>
            pack-gc true
            keep-history false
            <mysql>
                host %(host)s
                port %(port)s
                db %(mysqldb)s
                user %(mysqluser)s
                passwd %(mysqlpasswd)s
                %(socket-option)s
            </mysql>
        </relstorage>
        """ % _global_conf

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
        self._conn = DB(self._storage).open()
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

    def update_progress(self, finished, total):
        fraction = finished / float(total)
        bar_width = 40
        done = '=' * int(bar_width * fraction)
        undone = '-' * (bar_width - int(bar_width * fraction))
        sys.stderr.write('[%s%s%s] %s%% complete\r' % (done, '|', undone, int(fraction*100)))
        sys.stderr.flush()

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
            for k, v in parent.__dict__.iteritems():
                if v == child:
                    name = k
                    break
        return name, pickler.klass

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
        name, klass = self.analyze(*ancestors[-2:])
        sys.stderr.write(' '*80)
        sys.stderr.flush()
        print
        print "FOUND DANGLING REFERENCE"
        print "PATH", '/'.join(path)
        print "TYPE", parent_klass
        print "OID", repr(parent_oid), u64(parent_oid)
        print "Refers to a missing object:"
        print "    NAME", name
        print "    TYPE", klass
        print "    OID", repr(oid), u64(oid)
        print

    def verify(self, root):
        seen = set()
        seen_add = seen.add
        path = ()
        stack = deque([(root, path)])
        reported = 0
        while stack:
            oid, path = stack.popleft()
            seen_add(oid)
            if not len(seen) % 1000:
                self.update_progress(len(seen), self._size)
            try:
                state = self._storage.load(oid)[0]
            except POSKeyError:
                self.report(oid, path)
                reported += 1
            else:
                refs = get_refs(state)
                stack.extend((o, path + (o,)) for o in set(refs) - seen)
        return reported, len(seen), self._size


    def run(self):
        print
        print "="*50
        print
        print "   DATABASE INTEGRITY SCAN: ", self._dbname
        print
        print "="*50

        oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
        reported, scanned, total = self.verify(oid)

        sys.stderr.write(' '*80)
        sys.stderr.flush()

        print
        print "SUMMARY:"
        print "Found", reported, "dangling references"
        print "Scanned", scanned, "out of", total, "reachable objects"
        if total > scanned:
            print "(Run zenossdbpack to garbage collect unreachable objects)"
        print


def main():
    for db in ('zodb', 'zodb_session'):
        PKEReporter(db).run()


if __name__ == "__main__":
    main()


(lambda:Globals)() # quiet pyflakes
