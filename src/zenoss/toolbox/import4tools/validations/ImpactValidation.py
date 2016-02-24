##############################################################################
#
# Copyright (C) Zenoss, Inc. 2016, all rights reserved.
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

log = setupLogger(__name__)

MIN_ZP_VERS = '4.2.6'

class ImpactValidation(Import4Validation):
    """
    Validate that the currently installed version of Impact is compatible for
    export/import
    """
    @staticmethod
    def _add_parser(subparsers):
        parser = subparsers.add_parser('impact',
                                       description='Run validations against ' \
                                                   'Impact')
        return parser

    def validate(self, argz):
        dmd = ZenScriptBase(noopts=True, connect=True).dmd
        impactPack = dmd.ZenPackManager.packs.findObjectsById('ZenPacks.zenoss.Impact')
        if not impactPack:
            log.info('The Impact ZenPack is not installed')
            return

        impactPack = impactPack[0]
        if cmp(parse_version(impactPack.version), parse_version(MIN_ZP_VERS)) > 0:
            log.info('Impact ZenPack version %s is installed, OK to migrate', impactPack.version)
            return

        log.error('ZenPacks.zenoss.Impact is at version %s, but must be ' \
                  'at least version %s, exiting', impactPack.version, MIN_ZP_VERS)
        raise ValidationException()
