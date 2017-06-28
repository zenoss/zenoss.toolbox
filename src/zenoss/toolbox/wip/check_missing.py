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
import re

import kquery


def print_result_info(jdata=None):
    if "error" in jdata["responses"][0].keys():
        print "error: {}".format(jdata["responses"][0]["error"])
        return 0
    else:
        if "hits" not in jdata["responses"][0].keys():
            print "error - no hits returned"
            return 0
    hits = jdata["responses"][0]["hits"]["hits"]
    for h in hits:
        type = h["_source"]["fields"]["type"]
        message = h["_source"]["message"]
        print "{}\t{}\n".format(type, message)
    print "{} hits found.".format(len(hits))
    return len(hits)



def main(opts):
    data = parsefile(opts.file)
    tot_hits = 0
    s = kquery.get_session(opts.host, "root", "zenoss")
    if s is None:
        print "Unable to login"
        exit(1)
    for metric, key, timestamp in data:
        print "checking {}, {} at {}:".format(metric, key, timestamp)
        sstrings = metric.split('/')  # split device/metric
        sstrings.extend(key.split('/')[-1:])  # add interface from key
        result = kquery.query_kibana(opts.host, sstrings, timestamp, s)
        tot_hits += print_result_info(result)

    print "Total hits: {}".format(tot_hits)


def args():
    parser = argparse.ArgumentParser(
        description="parse findgaps6.py output into something check_missing.py can use.")
    parser.add_argument("-f", "--file", help="input file to parse")
    parser.add_argument("--host", default="hyang-tb10.zenoss.loc", help="host to query")
    return parser.parse_args()


def parsefile(filename):
    exp = r"gaps found in series (?P<metric>.*?)-(?P<key>.*): \[(?P<timestamps>.*)\]"
    result = []
    with open(filename, 'r') as f:
        for line in f:
            m = re.match(exp, line)
            if m is not None:
                metric = m.group('metric')
                key = m.group('key')
                timestamps = m.group('timestamps')
                result.extend(
                    [[metric, key, t] for t in timestamps.split(", ")])
    return result


if __name__ == '__main__':
    options = args()
    main(options)
