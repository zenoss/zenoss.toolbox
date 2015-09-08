##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

#!/opt/zenoss/bin/python

from . import Import4Validation, ValidationException
from .. import setupLogger

import Globals
from Products.ZenUtils.ZenScriptBase import ZenScriptBase

from pkg_resources import parse_version
import os
import inspect
import json

log = setupLogger(__name__)

MANIFEST_FILE = 'packmanifest.json'

UNKNOWN = 'UNKNOWN'
WARN = 'WARN'
OK = 'OK'

class WarnReason(object):

    REASONS = {
        'PACK_UPGRADE_NEEDED': 'The specified zenpack needs to be upgraded ' \
                    'to at least the specified version before ' \
                    'continuing with the export',
        'UNKNOWN': 'Pack in WARN state for an unknown reason'
    }

    def __init__(self, reason_text, data, packName):
        reason = WarnReason.REASONS.get(reason_text, None)
        if not reason:
            raise Exception('Warn reason unrecognized')
        self.text = reason
        self.data = data
        self.pack = packName


class ZenPackValidation(Import4Validation):
    """
    Validate that installed zenpacks are going to be compatible for
    export/import to a 5.x system.
    """
    @staticmethod
    def _add_parser(subparsers):
        parser = subparsers.add_parser('zenpack',
                                       description='Run validations against ' \
                                       'currently installed/available zenpacks')
        return parser

    def _getPacksFromManifest(self):
        manifestPath = '{0}/{1}'.format(
            os.path.dirname(inspect.getfile(ZenPackValidation)),
            MANIFEST_FILE
        )
        log.info("Manifest path is %s", manifestPath)
        with open(manifestPath, 'r') as fp:
            packManifest = json.load(fp)
        return packManifest['packs']

    def validate(self, argz):
        dmd = ZenScriptBase(noopts=True, connect=True).dmd
        ucsx = dmd.ZenPackManager.packs._getOb('ZenPacks.zenoss.UCSXSkin', None)
        if ucsx:
            self.doUcspmPackValidation(dmd, ucsx.version)
            log.info('ZenPack states are valid for upgrade')
            return
        manifestPacks = self._getPacksFromManifest()
        installedPacks = dmd.ZenPackManager.packs()

        results = {
            OK: [], # List of packs
            WARN: [], # List of WarnReasons
            UNKNOWN: [] # List of packs
        }

        def _getManifestPack(packName, manifestPacks=manifestPacks):
            return manifestPacks.get(packName, None)

        for pack in installedPacks:
            manifestPack = _getManifestPack(pack.id)
            # No record of pack in manifest
            if not manifestPack:
                results[UNKNOWN].append(pack.id)
                continue
            # Pack listed in manifest
            else:
                # 'WARN'
                if manifestPack['status'] == WARN:
                    reason = WarnReason('UNKNOWN', None, pack.id)
                    results[WARN].append(reason)
                # 'OK'
                elif manifestPack['status'] == OK:
                    manifestVersion = manifestPack['min4xVersion']
                    # There's a min 4x version required
                    if manifestVersion:
                        packVersion = pack.version
                        if cmp(parse_version(packVersion), parse_version(manifestVersion)) < 0:
                            reason = WarnReason('PACK_UPGRADE_NEEDED', manifestVersion, pack.id)
                            results[WARN].append(reason)
                            continue
                    results[OK].append(pack.id)
                # 'UNKNOWN' or unrecognized status
                else:
                    results[UNKNOWN].append(pack.id)

        if not results[WARN] + results[UNKNOWN]:
            log.info('ZenPack states are valid for upgrade')
            return

        if results[UNKNOWN]:
            msg = 'Found ZenPacks with unknown upgrade compatability - please contact support:'
            log.error(msg)
            for pack in results[UNKNOWN]:
                log.error(' ' * 4 + pack)

        if results[WARN]:
            log.error('Found ZenPacks that require action:')
            for reason in results[WARN]:
                log.error(' ' * 4 + reason.pack)
                log.error(' ' * 8 + reason.text)
                if reason.data:
                    log.error(' ' * 8 + reason.data)

        raise ValidationException()

    def doUcspmPackValidation(self, dmd, ucspmVersion):
        """
        Do a simpler zenpack validation.  Compare the installed zenpacks against
        the list of known packs.  Any deviation = fail.
        """

        class Pack(object):

            def __init__(self, name, version): # strings
                self.name = name
                self.version = version

            def __eq__(self, other):
                return self.__dict__ == other.__dict__

            def __hash__(self):
                return hash(self.name) + hash(self.version)

        # Get manifest
        manifestFiles = {
            '1.1.0': 'ucspm-110-packmanifest.csv',
            '1.1.1': 'ucspm-111-packmanifest.csv'
        }
        manifestPath = '{0}/{1}'.format(
            os.path.dirname(inspect.getfile(ZenPackValidation)),
            manifestFiles[ucspmVersion]
        )

        # collect installed packs + manifest packs
        manifestPacks = set()
        with open(manifestPath, 'r') as fp:
            for pack in fp:
                pack = pack.strip()
                name, version = pack.split(',')
                manifestPacks.add(Pack(name, version))
        installedPacks = set()
        for pack in dmd.ZenPackManager.packs():
            installedPacks.add(Pack(pack.id, pack.version))

        # process dupes in manifestPacks
        manifestPackNames = [pack.name for pack in manifestPacks]
        manifestDupes = set(filter(lambda x: manifestPackNames.count(x.name) > 1, manifestPacks))

        # compare the sets.  support duplicate packs in manifest, as long as we
        # have one of them installed.  Make sure that the installed packs are a
        # subset of the manifest packs AND manifestPacks.difference(installedPacks)
        # contains only packs that have multiple versions on the manifest (AKA
        # dupes, to support allowing a set of versions of a given pack)
        if not (installedPacks <= manifestPacks and \
                manifestPacks.difference(installedPacks) <= manifestDupes):
            log.error("Unexpected list of installed zenpacks found")
            log.error("Expected:")
            for pack in manifestPacks:
                log.error(4 * ' ' + "%s %s", pack.name, pack.version)
            log.error("Found:")
            for pack in installedPacks:
                log.error(4 * ' ' + "%s %s", pack.name, pack.version)
            raise ValidationException

