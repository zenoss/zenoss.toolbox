#!/usr/bin/env python

from __future__ import division

import collections
import datetime as dt
import operator
import signal
import subprocess
import sys

import requests


def debug_signal_handler(*_):
    import pdb
    pdb.set_trace()

signal.signal(signal.SIGINT, debug_signal_handler)

# from requests.packages.urllib3.exceptions import InsecureRequestWarning
# requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# THIS IS THE NUMBER OF DAYS BACK WE WILL QUERY
DAYS_BACK = 10


class MissingPoint(object):
    def __init__(self, metric, tag, timestamp):
        self.timestamp = timestamp
        self.metric = metric
        self.tag = tag


class GapFinder(object):
    def __init__(self, data, period=300, mingap=2, maxgap=10):
        self.data = sorted([int(x) for x in data.keys()])
        self.period = period
        self.mingap = mingap
        self.maxgap = maxgap

    def has_gaps(self):
        for gap in self.gap_summary():
            if (gap >= self.period * (self.mingap - 0.2) and
                    gap <= (self.maxgap + 0.2) * self.period):
                return True
        return False

    def gap_summary(self):
        return collections.Counter(self.gaps().values())
            
    def gaps(self):
        return {i: int(j)-int(i) for i, j in zip(self.data[:-1],
                                                 self.data[1:])}

    def gap_points(self):
        result = []
        for g in range(self.mingap, self.maxgap):
            gp = [x + (self.period * (g - 1))
                  for (x, y) in self.gaps().items()
                  if y > self.period * (g - 0.5)]
            result.extend(gp)
        return sorted(result)


def get_metric_names(pattern="."):
    OPENTSDB_CONTAINER = "writer"
    cmdstr = ('serviced service attach {}'
              ' /opt/opentsdb/build/tsdb uid'
              ' --config /opt/zenoss/etc/opentsdb/opentsdb.conf'
              ' grep {}')
    cmd = cmdstr.format(OPENTSDB_CONTAINER, pattern)
    p1 = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    output, err = p1.communicate()
    result = []
    for row in output.split('\n'):
        fields = row.split()
        result.append(fields)
    return [x[1][:-1] for x in result if len(x) > 1 and x[0] == 'metrics']


def get_datapoints_for_metric(metric_name, start=None, end=None, url=None,
                              timeout=15):
    if end is None:
        end = dt.datetime.utcnow()
    if start is None:
        start = end - dt.timedelta(days=1)
    if url is None:
        url = "https://opentsdb.morr-workstation/api/query"

    tfmt = "%Y/%m/%d-%H:%M:%S"
    querystring = ('{{ "start":"{}",'
                   '"end":"{}",'
                   '"queries": '
                   '[ {{ '
                   '"metric":"{}",'
                   '"aggregator":"sum",'
                   '"tags": {{ "key":"*" }} }} '
                   '] }}')
    data = querystring.format(start.strftime(tfmt),
                              end.strftime(tfmt),
                              metric_name)

    try:
        response = requests.post(url, data=data, verify=False, timeout=timeout)
        if response.status_code != requests.codes.ok:
            print "HTTP STATUS: {}".format(response.status_code)
            print "REQUEST DATA: {}".format(data)
            print "RESPONSE: {}".format(response.text)
            return None
    except requests.exceptions.Timeout:
        print "Request Timeout"
        print "REQUEST DATA: {}".format(data)
        return None

    json = response.json()
    return json


def process_metrics(metrics):
    start = dt.datetime.utcnow() - dt.timedelta(days=DAYS_BACK)
    a = MetricStatusAccumulator()
    n = len(metrics)
    i = 0
    tty = sys.stdout.isatty()
    for metric in metrics:
        i += 1
        if tty:
            print "Processing metric {:>12} of {:>12}\r".format(i, n),
            sys.stdout.flush()
        metric = metric.strip()
        data = get_datapoints_for_metric(metric, start, dt.datetime.utcnow())
        if data is None:
            continue
        for series in data:
            metric = series["metric"]
            key = series["tags"]["key"]
            a.points_seen(metric, key, len(series["dps"]))

            gf = GapFinder(series["dps"], mingap=2, maxgap=10)
            #print "\t{}-{}: {}".format(metric, key, gf.gap_summary())
            if gf.has_gaps():
                gap_points = gf.gap_points()
                print "gaps found in series {}-{}: {}".format(metric, key, gap_points)
                for point in gap_points:
                    a.missing_point(point, metric, key)
            for gs, gc in gf.gap_summary().items():
                a.all_gap_summary[gs] += gc
    print ""

    def print_missing(a):
        print "MISSING POINTS:"
        for (k, v) in a.missing_summary.items():
            print "\t{}:".format(k)
            for subv in v:
                print "\t\t{} - {}".format(subv[0], subv[1])
    
    def print_missing_summary(a):
        print "MISSING POINTS SUMMARY:"
        for (k, v) in sorted(a.missing_summary.items()):
            print "\t{:12}: {:>12}".format(k, len(v))

    print "METRICS: TOTAL: {}, COMPLETE: {}, WITH GAPS: {}".format(
        len(a.all_metrics),
        len(a.complete_metrics()),
        len(a.metrics_with_gaps))
    print "SERIES: TOTAL: {}, COMPLETE: {}, WITH GAPS: {}".format(
        len(a.all_series), len(a.complete_series()), len(a.series_with_gaps))
    a.print_missing_metrics(10)
    a.print_missing_series(30)
    print_missing_summary(a)
    print "TOTAL_POINTS_COLLECTED: {}".format(a.present_points)
    print "ALL GAPS SUMMARY BY SIZE: {}".format(sorted(dict(a.all_gap_summary).items(),
                                               key=operator.itemgetter(0),
                                               reverse=False))
    print "GAP SUMMARY BY FREQUENCY: {}".format(sorted(dict(a.all_gap_summary).items(),
                                               key=operator.itemgetter(1),
                                               reverse=True))
    lost_points = sum(a.missing_points.values())
    total_points = a.present_points + lost_points
    print "LOST {} of {} points ({}%)".format(lost_points, total_points,
                                              100 * lost_points /
                                              max(total_points, 1))
    


class MetricStatusAccumulator(object):
    def __init__(self):
        self.missing_points = collections.Counter()
        self.metrics_missing_counter = collections.Counter()
        self.series_missing_counter = collections.Counter()
        self.missing_mp = []
        self.total_points = 0
        self.present_points = 0
        self.all_metrics = set()
        self.all_series = set()
        self.metrics_with_gaps = set()
        self.series_with_gaps = set()
        self.missing_summary = collections.defaultdict(list)
        self.all_gap_summary = collections.defaultdict(int)

    def points_seen(self, metric, key, count):
        self.all_metrics.add(metric)
        self.all_series.add("{}-{}".format(metric, key))
        self.present_points += count
        self.total_points += count

    def missing_point(self, point, metric, key):
        self.total_points += 1
        self.metrics_with_gaps.add(metric)
        self.series_with_gaps.add("{}-{}".format(metric, key))
        self.missing_summary[point].append((metric, key))
        self.missing_points[point] += 1
        self.metrics_missing_counter[metric] += 1
        self.series_missing_counter[(metric, key)] += 1
        self.missing_mp.append(MissingPoint(metric, key, point))

    def complete_series(self):
        return self.all_series.difference(self.series_with_gaps)
    
    def complete_metrics(self):
        return self.all_metrics.difference(self.metrics_with_gaps)

    def print_missing_metrics(self, n):
        print "Top {} metrics with most missing points:".format(n)
        for metric, count in self.metrics_missing_counter.most_common(n):
            print "\t{}\t{}".format(count, metric)

    def print_missing_series(self, n):
        print "Top {} series with most missing points:".format(n)
        for series, count in self.series_missing_counter.most_common(n):
            print "\t{}\t{} - {}".format(count, series[0], series[1])



def main():
    pattern = "10."
    if len(sys.argv) > 1:
        pattern = sys.argv[1]
    print "Getting metric names for pattern {}".format(pattern)
    metrics = get_metric_names(pattern)
    print "metrics count: {}".format(len(metrics))
    process_metrics(metrics)

if __name__ == "__main__":
    main()
