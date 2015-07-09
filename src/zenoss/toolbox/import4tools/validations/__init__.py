##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

from .. import setupLogger

log = setupLogger(__name__)

# Whenever a validation is created, it's class needs to be added to this list
__all__ = [
    'ValidationException',
    'Import4Validation',

    'ZenPackValidation',
    'ImpactValidation'
]

class ValidationException(Exception):
    pass


class NotImplementedException(ValidationException):
    pass


class Import4Validation(object):
    """
    This is the base class for validations, to be subclassed for specific
    validations to run.  All methods that throw NotImplementedExceptions are
    expected to be overridden in subclasses
    """

    @classmethod
    def add_parser(clazz, subparsers):
        """
        This method is called from the parser-setter-upper to allow the
        validation subclass to add its own parser.  Do not edit this method, or
        override in a subclass.  Instead override _add_parser()

        :param subparsers: Action object normally created by
        ArgumentParser.add_subparsers()
        """
        parser = clazz._add_parser(subparsers)
        parser.set_defaults(clazz=clazz)

    @staticmethod
    def _add_parser(subparsers):
        """
        Method to add your validator's command parser.  You should construct
        a parser via calling add_parser() on the subparsers object.

        :param subparsers: Action object normally created by
        ArgumentParser.add_subparsers()
        :return: A constructed ArgumentParser
        :rtype: ArgumentParser
        """
        raise NotImplementedException()

    def validate(self, argz):
        """
        The validation that your validator will do.  To signal a failure slash
        invalid scenario slash something needs to be fixed, raise a ValidationException
        to validate's caller.  It is expected that any error logging is done
        by the implementor.

        :param argz: Arguments from a call to ArgumentParser.parse_args()
        :raises ValidationException: validation failed
        """
        raise NotImplementedException()
