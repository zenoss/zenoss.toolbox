zenoss.toolbox
==============
Utilities for analyzing and debugging Zenoss environments.


cleancatalog
------------
``cleancatalog`` will scan the global catalog and remove entries that
point to unresolvable paths. It is safe to run in the background.


zodbscan
--------
``zodbscan`` will scan the zodb and zodb_session databases, looking for
dangling references. If it finds one, it will analyze the pickled object
state to determine the path through the object graph, the name of the
attribute referencing the missing oid, and the class that should be there::

       ==================================================

           DATABASE INTEGRITY SCAN:  zodb

       ==================================================
                                                                                       
       FOUND DANGLING REFERENCE
       PATH /zport/dmd/Devices/devices/ian
       TYPE <class 'Products.ZenModel.Device.Device'>
       OID 0x0001ff38 '\x00\x00\x00\x00\x00\x01\xff8' 130872
       Refers to a missing object:
           NAME os
           TYPE <class 'Products.ZenModel.OperatingSystem.OperatingSystem'>
           OID 0x0001ff43 '\x00\x00\x00\x00\x00\x01\xffC' 130883
                                                                                       
       SUMMARY:
       Found 1 dangling references
       Scanned 96294 out of 96294 reachable objects

Author: Ian McCracken (ian@zenoss.com)

