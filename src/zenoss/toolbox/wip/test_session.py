#!/usr/bin/env python

import time
import warnings

import requests

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.8",
    # "cookie":"ZCPToken=VsPCSJHZ29kgaOM+U19NR1QyOIebT1xPR9M6AeAEICo=; ZUsername=root",
    "kbn-version": "4.6.4",
}


def get_query_string(strings, timestamp):
    # Kibana is really picky about formatting. Don't change whitespace on qt
    qt = """{{"index":["{}"],"ignore_unavailable":true}}
{{"size":500,"sort":[{{"@timestamp":{{"order":"desc","unmapped_type":"boolean"}}}}],"query":{{"filtered":{{"query":{{"query_string":{{"query":"mtrace{}","analyze_wildcard":true}}}},"filter":{{"bool":{{"must":[{{"range":{{"@timestamp":{{"gte":{},"lte":{},"format":"epoch_second"}}}}}}],"must_not":[]}}}}}}}},"fields":["*","_source"],"script_fields":{{}},"fielddata_fields":["@timestamp"]}}
"""
    sstr = ""
    if len(strings) > 0:
        l = [s.split('/') for s in strings]
        all = ["\\\"{}\\\"".format(item) for sublist in l for item in sublist]
        sstr = ' AND {}'.format(' AND '.join(all))

    tsval = int(timestamp)
    idx = time.strftime("logstash-%Y.%m.%d", time.gmtime(tsval))

    margin = 180
    ss = qt.format(idx, sstr, tsval - margin, tsval + margin)
    # import pdb; pdb.set_trace()
    return ss


url2 = "https://localhost/api/controlplane/kibana/elasticsearch/_msearch" \
       "?timeout=0&ignore_unavailable=true "


def query_kibana(strings, timestamp, s):
    npls = get_query_string(strings, timestamp)
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r = s.post(url2, data=npls, headers=headers, verify=False)
        if r.status_code != requests.codes.ok:
            print "HTTP STATUS: {}".format(r.status_code)
            print "REQUEST DATA: {}".format(npls)
            print "RESPONSE: {}".format(r.text)
            return None
    except requests.exceptions.Timeout:
        print "Request Timeout"
        print "REQUEST DATA: {}".format(npls)
        return None
    return r.json()


def main():
    s = requests.Session()
    data = "{\"Username\": \"zenny\", \"Password\": \"Z3n0ss\"}"
    url = "https://localhost/login"
    r = s.post(url, data=data, verify=False)
    print "result of login: {}".format(r)
    print "login result text: {}".format(r.text)
    print "cookies: {}".format(s.cookies)

    r = query_kibana([], "1496261812", s)
    print "result: {}".format(r)


if __name__ == '__main__':
    main()
