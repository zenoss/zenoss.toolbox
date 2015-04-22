#!/opt/zenoss/bin/python

# This module contains the validations to be run by the import4validations
# module.

import logging
log = logging.getLogger(__name__)


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
        The validation that your validator will do.

        :param argz: Arguments from a call to ArgumentParser.parse_args()
        """
        raise NotImplementedException()


class ZenPackValidation(Import4Validation):
    """
    Validate that installed zenpacks are going to be compatible for
    export/import to a 5.x system
    """

    @staticmethod
    def _add_parser(subparsers):
        parser = subparsers.add_parser('zenpack',
                                       description='Run validations against ' +
                                       'currently installed/available zenpacks')
        return parser

    def validate(self, argz):
        pass
