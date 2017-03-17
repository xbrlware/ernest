#!/usr/bin/env python

'''
Download new AQFS submissions from the EDGAR AQFS financial datasets page

** Note **
This runs prospectively using the --most-recent argument
'''

import json
import urllib2
import argparse

from datetime import date
from elasticsearch import Elasticsearch
from zipfile import ZipFile

# --
# CLI

parser = argparse.ArgumentParser(description='ingest_new_forms')
parser.add_argument("--from-scratch", action='store_true')
parser.add_argument("--most-recent", action='store_true')
parser.add_argument("--period", type=str, action='store', default='False')
parser.add_argument("--config-path",
                    type=str, action='store',
                    default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{'host': config['es']['host'],
                         'port': config['es']['port']}])


def __ingest(period):
    print('___ ingesting ___' + period)
    try:
        response = urllib2.urlopen(
            'https://www.sec.gov/data/financial-statements/' + period + '.zip')
    except urllib2.HTTPError:
        print('--quarterly document has not yet been released--')
        raise

    aqfs = response.read()

    with open('/home/ubuntu/data/xbrl_aqfs/' + period + '.zip', 'w') as outf:
        outf.write(aqfs)
        outf.close()

    with ZipFile('/home/ubuntu/data/xbrl_aqfs/' + period + '.zip', 'r') as zin:
        zin.extractall('/home/ubuntu/data/xbrl_aqfs/' + period + '/')

    with open('/home/ubuntu/data/xbrl_aqfs/' + period + '/sub.txt', 'r') as xin:
        x = xin.readlines()

    lst = []
    for line in x:
        row = line.split('\t')
        row[35] = row[35].replace('\n', '')
        lst.append(row)

    for i in range(1, len(lst)):
        x = lst[0]
        y = lst[i]
        dictionary = dict(zip(x, y))
        dictionary['file_period'] = period

        client.index(index="xbrl_submissions_cat",
                     doc_type='filing',
                     body=dictionary,
                     id=dictionary['adsh'])


def s_filter(yr):
    if yr < date.today().year:
        for qtr in range(1, 5):
            return str(yr) + 'q' + str(qtr)

    elif yr == date.today().year:
        for qtr in range(1, (int(date.today().month) / 3) + 1):
            return str(yr) + 'q' + str(qtr)

if __name__ == "__main__":
    p = []

    if args.from_scratch:
        p = [s_filter(yr) for yr in (range(2009, int(date.today().year) + 1))]

    elif args.most_recent:
        qtr = str(int(date.today().month) / 3)
        if qtr < 1:
            print('no new data available for first quarter')
        else:
            yr = str(int(date.today().year)) + 'q' + qtr
            p.append(yr)

    elif args.period == 'False':
        print('Choose argument from --period --from-scratch --most-recent')

    else:
        p.append(args.period)

    for period in p:
        __ingest(period)
