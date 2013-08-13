import logging

import Globals
from ZODB.transact import transact
from Products.ZenUtils.ZenScriptBase import ZenScriptBase

log = logging.getLogger("cleancatalog")

def cleanZodb(dmd):
    global_catalog = dmd.zport.global_catalog

    uncat = transact(global_catalog.uncatalog_object)

    for brain in global_catalog():
        try:
            obj = brain.getObject()
        except Exception:
            log.warn("Found unresolvable path, deleting: %s", brain.getPath())
            uncat(brain.getPath())

    log.info("Finished scanning catalog")
    
def main(): 
    dmd = ZenScriptBase(connect=True).dmd
    cleanZodb(dmd)

if __name__ == "__main__":
    main()

