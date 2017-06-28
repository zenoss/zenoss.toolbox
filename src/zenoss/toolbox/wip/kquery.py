#!/usr/bin/env python

import argparse
import getpass
import json
import logging
import sys
import time
import warnings
from datetime import datetime
from datetime import timedelta

import pytz
import requests
from dateutil import parser

# from requests.packages.urllib3.exceptions import InsecureRequestWarning

# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.8",
    # "kbn-version": "4.5.2",  # try to find a way not to need this.
    "kbn-version": "4.6.4",
}


def make_index_array_old(tsfrom, tsto, pattern):
    def perdelta(start, end, delta):
        curr = start
        while curr <= end:
            yield curr
            curr += delta

    return [x.strftime(pattern) for x in
            perdelta(tsfrom, tsto, timedelta(days=1))]


def make_index_array(dt_from, dt_to, pattern):
    def perdelta(start, end, delta):
        curr = start
        while curr <= end:
            yield curr
            curr += delta

    tsfrom = parser.parse(dt_from)
    tsto = parser.parse(dt_to)

    return [x.strftime(pattern) for x in
            perdelta(tsfrom, tsto, timedelta(days=1))]


def epoch_seconds(dt_str):
    try:
        tsdt = parser.parse(dt_str)
    except ValueError:
        logging.error("Could not parse date {}".format(dt_str))
        return None

    epoch = datetime(1970, 1, 1, tzinfo=pytz.UTC)
    return int((tsdt - epoch).total_seconds())


def get_query_string(strings, dt_from, dt_to):
    logging.debug(
        "get_query_string({}, {}, {})".format(strings, dt_from, dt_to))
    # Kibana is hyper-picky about formatting, so do not change whitespace
    # on this string
    qt = """{{"index":[{}],"ignore_unavailable":true}}
{{"size":10000,"sort":[{{"@timestamp":{{"order":"desc","unmapped_type":"boolean"}}}}],"query":{{"filtered":{{"query":{{"query_string":{{"query":"mtrace{}","analyze_wildcard":true}}}},"filter":{{"bool":{{"must":[{{"range":{{"@timestamp":{{"gte":{},"lte":{},"format":"epoch_second"}}}}}}],"must_not":[]}}}}}}}},"fields":["*","_source"],"script_fields":{{}},"fielddata_fields":["@timestamp"]}}
"""
    sstr = ""
    if len(strings) > 0:
        l = [s.split('/') for s in strings]
        all = ["\\\"{}\\\"".format(item) for sublist in l for item in sublist]
        sstr = ' AND {}'.format(' AND '.join(all))

    # try:
    #     tsfrom = parser.parse(dt_from)
    # except ValueError:
    #     logging.error("Could not parse from date ({})".format(dt_from))
    #     return None
    # try:
    #     tsto = parser.parse(dt_to)
    # except ValueError:
    #     logging.error("Could not parse to date({})".format(dt_to))
    #     return None
    #

    # todo: make idx a list - one for each day in interval?
    # idxary = make_index_array(tsfrom, tsto, "logstash-%Y.%m.%d")
    idxary = make_index_array(dt_from, dt_to, "logstash-%Y.%m.%d")
    idx = ', '.join('"{0}"'.format(i) for i in idxary)

    # ss = qt.format(idx, sstr, tsfrom.strftime('%s'), tsto.strftime('%s'))
    ss = qt.format(idx, sstr, epoch_seconds(dt_from), epoch_seconds(dt_to))
    return ss


def make_window(timestamp, margin):
    tsval = int(timestamp)
    tsfrom = tsval - margin
    tsto = tsval + margin
    return tsfrom, tsto


def query_kibana(host="localhost", strings=[], dt_from=None, dt_to=None,
                 s=None):
    logging.debug("query_kibana({}, {}, {}, {})".format(host, strings, dt_from,
                                                        dt_to))
    url = "https://{}/api/controlplane/kibana/elasticsearch/_msearch?" \
          "timeout=0&ignore_unavailable=true".format(
        host)

    npls = get_query_string(strings, dt_from, dt_to)
    logging.debug("query string: {}".format(npls))
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r = s.post(url, data=npls, headers=headers, verify=False)
        if r.status_code != requests.codes.ok:
            logging.error("Post Failed. url: {}, Status: {}, Request: {},"
                          " Response: {}".format(url, r.status_code, npls,
                                                 r.text))
            print "HTTP STATUS: {}".format(r.status_code)
            print "REQUEST DATA: {}".format(npls)
            print "RESPONSE: {}".format(r.text)
            return None
    except requests.exceptions.Timeout:
        logging.error("request timed out.")
        print "Request Timeout"
        print "REQUEST DATA: {}".format(npls)
        return None
    return r.json()


def get_session(host, user, password):
    logging.debug("get_session({}, {}, {})".format(host, user, password))
    s = requests.Session()
    if user is None:
        user = raw_input("Kibana user:")
    if password is None:
        password = getpass.getpass("Kibana password:")
    data = "{{\"Username\": \"{}\", \"Password\": \"{}\"}}".format(user,
                                                                   password)
    url = "https://{}/login".format(host)
    with warnings.catch_warnings():
        r = s.post(url, data=data, verify=False)
        if r.status_code != requests.codes.ok:
            logging.error("Post Failed. Host: {}, Status: {}, Request: {},"
                          " Response: {}".format(host, r.status_code, data,
                                                 r.text))
            print "HTTP STATUS: {}".format(r.status_code)
            print "REQUEST DATA: {}".format(data)
            print "RESPONSE: {}".format(r.text)
            return None
    logging.debug("get_session successful")
    return s


default_host = "hyang-tb10.zenoss.loc"
default_user = "root"
default_password = "zenoss"


def args():
    ap = argparse.ArgumentParser(
        description="query kibana on host.")
    ap.add_argument("--host", default=default_host,
                    help="host to query")
    ap.add_argument("-u", "--user", default=default_user, help="username")
    ap.add_argument("-p", "--password", default=default_password,
                    help="password")
    ap.add_argument("-s", "--strings", nargs='+', help="query strings")
    ap.add_argument("-t", "--timestamp", default=time.time(),
                    help="timestamp")
    ap.add_argument("-v", "--verbose", help="Enable verbose output",
                    action="store_true")
    ap.add_argument("-r", "--pretty_print", action="store_true")
    return ap.parse_args()


def main(opts):
    logfn = "kquery-{}.log".format(time.strftime('%Y%m%d-%H%M%S'))
    if opts.verbose:
        logging.basicConfig(filename=logfn, level=logging.DEBUG)
        logging.debug("Verbose!")
    else:
        logging.basicConfig(filename=logfn, level=logging.INFO)

    logging.debug("getting session on {} as {}".format(opts.host, opts.user))
    s = get_session(opts.host, opts.user, opts.password)
    if s is None:
        print >> sys.stderr, "Unable to get session"
        return -1

    tsfrom, tsto = make_window(opts.timestamp, 185)
    j = query_kibana(opts.host, opts.strings, )
    j = query_kibana(opts.host, opts.strings, opts.timestamp, s)
    if opts.pretty_print:
        print json.dumps(j, indent=4, sort_keys=True)
    else:
        print json.dumps(j)


if __name__ == '__main__':
    opts = args()
    main(opts)
