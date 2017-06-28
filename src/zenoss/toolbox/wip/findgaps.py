#!/usr/bin/env python

##############################################################################
#
# Copyright (C) Zenoss, Inc. 2017, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
##############################################################################

import argparse
import logging
import time
from collections import Counter
from pprint import pprint

import kquery


# params: host, start time, end time, metric filter
def args():
    parser = argparse.ArgumentParser(
        description="query kibana logs for mtrace items and look for data "
                    "points that did not complete the entire pipeline")
    parser.add_argument("--host", default="localhost", help="host to query")
    parser.add_argument("--dt_from", help="start time to query")
    parser.add_argument("--dt_to", help="end time to query")
    parser.add_argument("--user", help="Username for Kibana")
    parser.add_argument("--password", help="Password for Kibana")
    parser.add_argument("--verbose", help="Enable verbose logging",
                        action="store_true")
    data = parser.parse_args()
    return data


# connect to Kibana, read logs for time period (filter on mtrace)
# parse logs, print out any anomalies
#   anomalies: anything that is not the expected 5 messages for happy path
# For each anomaly:
#    print metric name, key, timestamp, messages (all)
# Parsed data storage: [


def readlogs(host, user, password, dt_from, dt_to):
    logging.debug(
        "readlogs({},{},{}".format(host, user, password, dt_from, dt_to))
    s = kquery.get_session(host, user, password)
    logdata = kquery.query_kibana(host, [], dt_from, dt_to, s=s)
    return logdata


def parse_logquery_data(data):
    result = []
    for r in data['responses']:
        for h in r['hits']['hits']:
            result.append(
                {'index': h['_index'], 'type': h['_source']['fields']['type'],
                 'message': h['_source']['message']})
    return result


def main(opts):
    logfn = "findgaps-{}.log".format(time.strftime('%Y%m%d-%H%M%S'))
    if opts.verbose:
        logging.basicConfig(filename=logfn, level=logging.DEBUG)
        logging.debug("Verbose!")
    else:
        logging.basicConfig(filename=logfn, level=logging.INFO)

    logging.getLogger().addHandler(logging.StreamHandler())

    logging.info("Logging enabled.")
    logging.debug("getting session on {} as {}".format(opts.host, opts.user))
    data = readlogs(opts.host, opts.user, opts.password, opts.dt_from,
                    opts.dt_to)
    logging.debug("got data")
    messages = parse_logquery_data(data)
    logging.debug("parsed data")
    ictr = Counter()
    tctr = Counter()
    mct = 0
    for m in messages:
        ictr[m['index']] += 1
        tctr[m['type']] += 1
        mct += 1

    print("mct: {}", mct)

    pprint(messages)

    print "Indexes:"
    pprint(ictr)
    print "Types:"
    pprint(tctr)
    # logging.debug("Data:\n{}".format(data))


if __name__ == '__main__':
    options = args()
    main(options)
