zenoss.toolbox
==============
Contains utilities for analyzing and debugging Zenoss environments.  To install the latest version of the zenoss.toolbox:

1) Login to the zenoss master
2) Become the zenoss user:
   su - zenoss
3) Download the latest zenoss.toolbox:
   wget -O master.zip --no-check-certificate https://github.com/zenoss/zenoss.toolbox/archive/master.zip
4) Use python's easy_install to install the zenoss.toolbox:
   easy_install ./master.zip

==============
 
findposkeyerror
---------------
``findposkeyerror`` will scan the supplied zodb path, looking for
dangling references for certain objects and fix those if --fixrels
is supplied as a command argument.

zencatalogscan
--------------
``zencatalogscan`` will scan the various zenoss catalogs for any references
that point to unresolvable paths.  It is safe to run this in the background.
If you run with "-f" or "--fix" it will attempt to uncatalog any references
that are no longer present.  

usage: zencatalogscan.py [-h] [-v] [-l] [-f] [-n N] [-c CATALOG]

Scans catalogs for references to missing objects. Before using zencatalogscan
you must first confirm both zodbscan & findposkeyerrors return clean.

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -l, --list            list all catalogs supported for scan
  -f, --fix             attempt to remove any invalid references
  -n N, --N N           maximum times to cycle (with --fix)
  -c CATALOG, --catalog CATALOG
                        select a single catalog to scan/fix

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

Author: Brian Bibeault (bbibeault@zenoss.com)
