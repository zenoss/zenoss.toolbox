#!/usr/bin/env python
##############################################################################
#
# Copyright (C) Zenoss, Inc. 2017, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import ast
import re
import shlex

import dateutil.parser
import pytz


def parse_mtrace(message):
    pass


def parse_metricshipper_message(msg):
    result = {}

    # parse out tags
    m = re.match('(.*) tags=map\[(.*)\](.*)', msg)
    if m is not None:
        g = m.groups()
        tags = shlex.split(g[1])
        td = {t[0]: t[1] for t in (tag.split(':', 1) for tag in tags)}
        result['tags'] = td
        msg = g[0] + g[2]

    fields = shlex.split(msg)
    fd = {f[0]: f[1] for f in (field.split('=', 1) for field in fields)}
    result.update(fd)

    result['logtime'] = result.pop('time')
    result['log_datetime'] = dateutil.parser.parse(result['logtime'])
    return result


def parse_metricconsumer_message(msg):
    result = {}

    msgclean = msg.replace('\n', '')
    mc_regex = r"(?P<level>\w*)" \
               r"\W*\[" \
               r"(?P<logtime>.+?)" \
               r"\] " \
               r"(?P<class>.*?)" \
               r": " \
               r"mtrace=" \
               r"(?P<mtrace>[\d,.]*)" \
               r" elapsed=" \
               r"(?P<elapsed>[\d,.]*)" \
               r" " \
               r"message=" \
               r"(?P<message>.*)" \
               r"\Wmetric=\[" \
               r"(?P<metric>.*)" \
               r"\]"

    m = re.match(mc_regex, msgclean)
    if m is not None:
        result.update(m.groupdict())
    else:
        print "error - metricconsumer message did not match regular " \
              "expression."
        print "message: {}".format(msg)
        print "regex: {}".format(mc_regex)

    # parse out tags
    m = re.search('tags={(.*?)}', msg)
    if m is not None:
        g = m.groups()
        tags = shlex.split(g[0])
        td = {t[0]: t[1].rstrip(',') for t in
              (tag.split('=', 1) for tag in tags)}
        result['tags'] = td

    result['log_datetime'] = dateutil.parser.parse(result['logtime']).replace(
        tzinfo=pytz.UTC)

    return result


def parse_zenperfsnmp_message(msg):
    result = {}
    rx = r"(?P<logtime>[\d,\-: ]*) " \
         r"(?P<level>\w*)" \
         r"\W" \
         r"(?P<class>[\w\.]*)" \
         r":\W" \
         r"mtrace:\W" \
         r"(?P<message>.*)" \
         r"\Wmetric (?P<metric>[\w\./]*)\W" \
         r"(?P<value>[\d\.]*)" \
         r"\W" \
         r"(?P<timestamp>[\d\.]*)" \
         r"\W" \
         r"(?P<tags>{.*})"
    m = re.match(rx, msg)
    if m is not None:
        result.update(m.groupdict())
    else:
        print "error - zenperfsnmp message did not match regular expression."
        print "message: {}".format(msg)
        print "regex: {}".format(rx)

    # parse out tags
    tags = ast.literal_eval(result['tags'])
    result['tags'] = tags

    result['mtrace'] = result['tags']['mtrace']
    result['log_datetime'] = dateutil.parser.parse(result['logtime']).replace(
        tzinfo=pytz.UTC)
    return result


message_parsers = {
    'metricshipper': parse_metricshipper_message,
    'metricconsumer': parse_metricconsumer_message,
    'zenperfsnmp': parse_zenperfsnmp_message
}


class Mtrace:
    def __init__(self, hit):
        self.hit = hit

    def get_mtrace(self):
        parser = message_parsers[self.get_fieldtype()]
        print parser
        message = self.get_message()
        return parser(message)['mtrace']

    def get_message(self):
        return self.hit['_source']['message']

    def get_level(self):
        parser = message_parsers[self.get_fieldtype()]
        print parser
        message = self.get_message()
        return parser(message)['level'].upper()

    def get_fieldtype(self):
        return self.hit['_source']['fields']['type']

    def get_msg_date(self):
        parser = message_parsers[self.get_fieldtype()]
        message = self.get_message()
        return parser(message)['log_datetime']
