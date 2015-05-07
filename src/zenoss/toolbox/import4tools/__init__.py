##############################################################################
#
# Copyright (C) Zenoss, Inc. 2015, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import logging
import sys


def setupLogger(loggerName):
    log = logging.getLogger(loggerName)
    log.setLevel(logging.INFO)
    return log

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s')

log = setupLogger(__name__)
