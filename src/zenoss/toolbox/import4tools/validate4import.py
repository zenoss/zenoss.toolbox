#!/opt/zenoss/bin/python

# This script is the main entrypoint for validation code.  To add a validation
# to this process, subclass Import4Validation in the validations module and
# import it here

from validations import *

from argparse import ArgumentParser
import sys
import logging

log = logging.getLogger(__name__)


class ValidationRunner(object):
    """
    This class is the driver for the parsed/chosen validation.  The main()
    method of this module will instantiate a single runner and then call its
    'run' method, which will return an integer exit code
    """

    def __init__(self, argz):
        self.vTask = argz.clazz()
        self.argz = argz

    def run(self):
        try:
            log.info("Starting validation %s", self.vTask.__class__.__name__)
            self.vTask.validate(self.argz)
            log.info("Validation successful")
            return 0
        except Exception as e:
            log.exception(e)
            log.error("Validation failed")
            return -1


def setup_parser(validationSubs):
    parser = ArgumentParser(
        description="Validation tool used to run validations prior to " +
        "exporting data for a version 5 import"
    )
    subparsers = parser.add_subparsers()
    for validationSub in validationSubs:
        validationSub.add_parser(subparsers)
    return parser


def main():
    parser = setup_parser(type.__subclasses__(Import4Validation))
    argz = parser.parse_args()
    runner = ValidationRunner(argz)
    sys.exit(runner.run())

if __name__ == '__main__':
    main()
