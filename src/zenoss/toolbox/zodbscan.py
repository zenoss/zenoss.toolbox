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

from Products.ZenUtils.AutoGCObjectReader import gc_cache_every
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

    def persistent_load(self, pickle_id):
        if isinstance(pickle_id, tuple):
            oid, klass = pickle_id
            if oid == self.problem_oid:
                self.klass = klass
                return self._marker
        else:
            pass
            #try:
            #    oid_u64, oid_0x, oid_rep = PKEReporter.oid_versions(pickle_id)
            #    print "### WARNING: pickle_id is not tuple - oid:", oid_0x, oid_rep, oid_u64
            #except Exception:
            #    # what the heck is pickle_id?
            #    print "### ERROR: pickle_id not tuple:", repr(pickle_id)

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
    conf = getGlobalConfiguration()

    if database:
        conf['zodb-db'] = conf['mysqldb'] = database
    else:
        conf['mysqldb'] = conf.get('mysqldb', conf.get('zodb-db'))
        conf['zodb-db'] = conf.get('zodb-db', conf.get('mysqldb'))

    zodb_socket = conf.get('mysqlsocket',
                                   conf.get('zodb-socket'))

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
        sys.stderr.write(' '*80)
        sys.stderr.flush()
        par_u64, par_0x, par_rep = self.oid_versions(parent_oid)
        oid_u64, oid_0x, oid_rep = self.oid_versions(oid)
        print """
FOUND DANGLING REFERENCE
PATH {path}
TYPE {type}
OID {par_0x} {par_rep} {par_u64}
Refers to a missing object:
    NAME {name}
    TYPE {klass}
    OID", {oid_0x} {oid_rep} {oid_u64}
""".format(path='/'.join(path), type=parent_klass, name=name, klass=klass,
          par_u64=par_u64, par_0x=par_0x, par_rep=par_rep,
          oid_u64=oid_u64, oid_0x=oid_0x, oid_rep=oid_rep)

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
        with gc_cache_every(1000, self._db):
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
