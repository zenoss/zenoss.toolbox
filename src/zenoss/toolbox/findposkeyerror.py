#!/usr/bin/env python

import abc
import argparse
import logging
import re
import transaction
from ZODB.POSException import POSKeyError
from ZODB.utils import u64

import Globals
from Products.ZenRelations.ToManyContRelationship import ToManyContRelationship
from Products.ZenRelations.RelationshipBase import RelationshipBase
from Products.ZenUtils.Utils import unused
from Products.ZenUtils.ZenScriptBase import ZenScriptBase

from ZenPacks.zenoss.AdvancedSearch.SearchManager import SearchManager, SEARCH_MANAGER_ID

unused(Globals)


class Fixer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def fixable(self, ex, objId, parentPath):
        """
        Return a no-argument callable object that will perform the fix
        when invoked or None if not fixable.
        """


class RelFixer(Fixer):

    def fixable(self, ex, relId, parentPath):
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
                print("  ==-- cannot fix this relationship; "
                    "no _objects attribute")
                return None
            exOID = getOID(ex)
            relOID = getPOID(relationship._objects)
            if exOID == relOID:
                return lambda: self._fix(exOID, relOID, relationship, parent)
            else:
                print("  ==-- cannot fix this relationship; "
                    "exOID: %s != relOID: %s" % (exOID, relOID))
        except:
            return None

    def _fix(self, exOID, relOID, relationship, parent):
        """
        Fix the exception.
        """
        print "  ==-- fixing POSKeyError - exOID:%s == relOID:%s" \
            % (exOID, relOID)
        cls = relationship._objects.__class__
        relationship._objects = cls()
        parent._p_changed = True
        transaction.commit()


class SearchManagerFixer(Fixer):
    # POSKeyError: 0x0683923b on attribute 'SearchManager' of app.zport.dmd.ZenUsers.svs
    # >>> dmd.ZenUsers.svs.SearchManager.__class__
    # <class 'ZenPacks.zenoss.AdvancedSearch.SearchManager.SearchManager'>
    # >>> find('svs')
    # <UserSettings at /zport/dmd/ZenUsers/svs>
    # >>> d=_
    # >>> d._delOb('SearchManager')
    # >>> commit()

    # Both of the following two methods are untested:
    def fixable(self, ex, objId, parentPath):
        """
        Return True if this object can fix the exception.
        """
        if objId != 'SearchManager':
            return None
        parent = dmd.getObjByPath(parentPath)
        obj = parent._getOb(objId)
        if not isinstance(obj, SearchManager):
            return None
        exOID = getOID(ex)
        relOID = getPOID(obj)
        if exOID == relOID:
            return lambda: self._fix(exOID, parent)
        return None

    def _fix(self, exOID, parent):
        """
        Fix the exception.
        """
        print "  ==-- fixing POSKeyError - exOID:%s" % (exOID)
        # delete only; a new one will be created when a SearchProvider
        # is requested.
        parent._delOb('SearchManager')
        parent._setObject(SEARCH_MANAGER_ID, SearchManager(SEARCH_MANAGER_ID))
        transaction.commit()


class ComponentSearchFixer(Fixer):
    # POSKeyError: 0x070039e0 on attribute 'componentSearch' of app.zport.dmd.Devices.Network.Juniper.mx.mx_240.devices.edge1.fra
    # >>> dmd.Devices.Network.Juniper.mx.mx_240.devices._getOb('edge1.fra')
    # <Device at /zport/dmd/Devices/Network/Juniper/mx/mx_240/devices/edge1.fra>
    # >>> d=_
    # >>> d._delOb('componentSearch')
    # >>> d._create_componentSearch()
    # >>> commit()

    # Both of the following two methods are untested:
    def fixable(self, ex, objId, parentPath):
        """
        Return True if this object can fix the exception.
        """
        if objId != 'componentSearch':
            return None
        parent = dmd.getObjByPath(parentPath)
        obj = parent._getOb(objId)
        exOID = getOID(ex)
        relOID = getPOID(obj)
        if exOID == relOID:
            return lambda: self._fix(exOID, parent)
        return None

    def _fix(self, exOID, parent):
        """
        Fix the exception.
        """
        print "  ==-- fixing POSKeyError - exOID:%s" % (exOID)
        parent._delOb('componentSearch')
        parent._create_componentSearch()
        transaction.commit()


# TODO: after peer review, add to _fixits list: ComponentSearchFixer(),
_fixits = [
    RelFixer(), SearchManagerFixer(),
]


def _getEdges(node):
    cls = node.aq_base
    names = set(node.objectIds() if hasattr(cls, "objectIds") else [])
    relationships = set(
        node.getRelationshipNames()
        if hasattr(cls, "getRelationshipNames") else []
    )
    return (names - relationships), relationships


_RELEVENT_EXCEPTIONS = (POSKeyError, KeyError, AttributeError)


def _getPathStr(path):
    return "app%s" % ('.'.join(path)) if len(path) > 1 else "app"


def findPOSKeyErrors(topnode):
    """
    Returns a generator that yields tuples containing these values:
        Name of exception type object,
        Exception,
        Type of problem object,
        Name (ID) of the object,
        The path to the parent of the named object

    """
    # objects that will have its children traversed are stored in 'nodes'
    nodes = [topnode]
    # While there are nodes to traverse
    while nodes:
        node = nodes.pop(0)
        path = node.getPhysicalPath()
        # Retrieve the names of all the child nodes and relationships.
        # Must handle relationship objects differently.
        try:
            attributes, relationships = _getEdges(node)
        except _RELEVENT_EXCEPTIONS as e:
            yield (
                type(e).__name__, e,
                "while retreiving children of", name, path
            )
            continue
        # Check the relationship objects
        for name in relationships:
            try:
                rel = node._getOb(name)
                rel()
            except _RELEVENT_EXCEPTIONS as e:
                #import pdb
                #pdb.set_trace()
                yield (type(e).__name__, e, "relationship", name, path)
            else:
                # ToManyContRelationship objects should be traversed to
                # locate additional objects.
                if isinstance(rel, ToManyContRelationship):
                    nodes.append(rel)
        # Check the non-relationship child objects
        for name in attributes:
            try:
                childnode = node._getOb(name)
                childnode.getId()
            except _RELEVENT_EXCEPTIONS as e:
                yield (type(e).__name__, e, "attribute", name, path)
            else:
                # No exception, so it should be safe to add this child node
                # as a traversable object.
                nodes.append(childnode)


def fixPOSKeyError(exname, ex, objType, objId, parentPath):
    """
    Fixes POSKeyErrors given:
        Name of exception type object,
        Exception,
        Type of problem object,
        Name (ID) of the object,
        The path to the parent of the named object

    """
    #if objType != "relationship":
    #    return
    #print "  ==-- attempting to fix POSKeyErrors for ZenRelationship object"

    # -- verify that the OIDs match
    for fixer in _fixits:
        fix = fixer.fixable(ex, objId, parentPath)
        if fix:
            fix()
            break


def getPOID(obj):
    # from ZODB.utils import u64
    return "0x%08x" % u64(obj._p_oid)

def getOID(ex):
    return "0x%08x" % int(str(ex), 16)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find POSKeyErrors 1.4")
    parser.add_argument(
        "folder", metavar="PATH", type=str,
        help="Object path where to start searching from.  E.g. Devices.Server"
    )
    parser.add_argument(
        "--fixrels", action="store_true", default=False,
        help="Automatically fix ZenRelationship objects"
    )
    args = parser.parse_args()
    #import pdb; pdb.set_trace()

    # Configure NullHandler for logging to suppress 'no handler for
    # logger' messages.
    logger = logging.getLogger()
    logger.addHandler(logging.NullHandler())

    ZenScriptBase.doesLogging = False  # disable logging configuration
    dmd = ZenScriptBase(noopts=True, connect=True).dmd
    # Split along '/' and '.' delimiters
    path = re.split("[./]", args.folder)
    # If the first path element is 'app' (name of root node in zendmd),
    # then remove it from the path, because that name doesn't actually
    # exist in the database.
    if path[0] == "app":
        path = path[1:]
    # Rebuild path using '/' delimiter
    path = '/'.join(path) if path else '/'
    try:
        folder = dmd.getObjByPath(path)
    except KeyError:
        print "Invalid path: %s" % (args.folder,)
    else:
        for exname, ex, objType, objId, parentPath in findPOSKeyErrors(folder):
            print "%s: %s on %s '%s' of %s" \
                % (exname, ex, objType, objId, _getPathStr(parentPath))
            # example == POSKeyError: 0x0118ef28 on relationship 'dependents' of app.zport.dmd.Devices.VMware.TNL1DMZVC01.Hosts.devices.TNL1DMZVC01_host-125.hw
            if args.fixrels:
                if isinstance(ex, POSKeyError):
                    fixPOSKeyError(exname, ex, objType, objId, parentPath)
