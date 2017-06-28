#!/usr/bin/env python

import argparse
import re


def args():
    parser = argparse.ArgumentParser(
        description="parse findgaps6.py output into something check_missing.py can use.")
    parser.add_argument("-f", "--file", help="input file to parse")
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
                result.extend([[metric, key, t] for t in timestamps.split(", ")])
    return result

def main(opts):
    result = parsefile(opts.file)
    for r in result:
        print r


if __name__ == '__main__':
    options = args()
    main(options)
