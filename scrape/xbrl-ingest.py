#!/usr/bin/env python

'''
    Cleans and ingests parsed xbrl filings downloaded by the
    xbrl-download script

    ** Note **
    This runs prospectively each day for the current year and
    month as new filings are downloaded
'''

import os.path
import os
import csv
import re
import json
import argparse
import itertools

from elasticsearch import Elasticsearch
from datetime import datetime

from modules.tags import generate_tags
from modules.dei_tags import generate_dei_tags
from generic.logger import LOGGER


def dei_tree(dei_frame):
    k = dei_frame
    k.sort()
    c = list(k for k, _ in itertools.groupby(k))
    dei_tree = {}
    for i in c:
        dei_tree[i[2]] = {
            'fact': i[4],
            'from_date': find_date(i[12]),
            'to_date': find_date(i[13])
            }
    return dei_tree


def fact_tree(tag_frame):
    z = tag_frame
    tree = {}
    for i in z:
        tree[i[2]] = {
            i[1]: {
                'fact': i[4],
                'unit': i[3],
                'decimals': i[5],
                'scale': i[6],
                'sign': i[7],
                'factId': i[8],
                'from_date': i[9],
                'to_date': i[10]
            }
        }
    return tree


def append_tree(i_tag_frame):
    return {
        "value": i_tag_frame[4],
        "context": i_tag_frame[1],
        "from": find_date(i_tag_frame[9]),
        "to": find_date(i_tag_frame[10])
    }


def fact_list(tag_frame, entry):
    z = tag_frame
    tree = {}
    for i in z:
        if i[13] != entry['entity_info']['dei_DocumentType']['to_date']:
            pass
        else:
            try:
                x = tree[i[2]]
            except:
                tree[i[2]] = append_tree(i)
            x = tree[i[2]]
            if toDate(x["from"]) > toDate(i[9]):
                tree[i[2]] = append_tree(i)
            elif toDate(x["from"]) == toDate(i[9]):
                if len(x['context']) > len(i[1]):
                    tree[i[2]] = append_tree(i)
    return tree


def find_date(date):
    l = []
    if date != "NA":
        l = re.findall(re.compile("\d{4}-\d{2}-\d{2}"), date)
    if len(l) < 1:
        l = ['1900-01-01']
    return l[0]


def toDate(date):
    if date == "NA":
        date = datetime.strptime("1900-01-01", "%Y-%m-%d")
    else:
        date = datetime.strptime(find_date(date), "%Y-%m-%d")
    return date


def build_object(frame):
    out = []
    for c in range(0, len(frame)):
        x = frame[c]
        if 'TextBlock' in x[2] or 'Axis' in x[1]:
            pass
        else:
            try:
                if int(x[0]):
                    if len(re.findall('-', x[12])) == 2:
                        out.append(x)
                    else:
                        pass
                else:
                    pass
            except (ValueError, IndexError):
                pass
    return out


def ingest(year, month):
    path = '/home/ubuntu/sec/parsed_min__' + year + '__' + month.zfill(2)
    for x in os.listdir(path):
        try:
            doc = path + '/' + x
            f = open(doc, 'rU')
            reader = csv.reader(f)
            rows = list(reader)
            entry = {
                "link": x,
                "year": year,
                "month": str(month).zfill(2),
                "entity_info": {},
                "facts": {}
            }
            # define list inputs
            f = []
            for i in range(1, len(rows)):
                try:
                    row = [rows[i][k] for k in range(0, 14)]
                    f.append(row)
                except:
                    pass

            dei_frame = build_object(
                [f[i] for i in range(0, len(f)) if f[i][2] in dei_tags])
            tag_frame = build_object(
                [f[i] for i in range(0, len(f)) if f[i][2] in tags])
            # structure doc entity information
            entry['entity_info'] = dei_tree(dei_frame)
            # eliminate non 10-K / 10-Q docs
            try:
                p = entry['entity_info']['dei_DocumentType']
                if p['fact'] in ['10-K', '10-Q']:
                    entry['facts'] = fact_list(tag_frame, entry)
                    client.index(index='ernest_xbrl_rss',
                                 doc_type='filing',
                                 body=entry,
                                 id=x)
            except KeyError:
                logger.error('{0}|{1}'.format(
                    'KEYERROR', 'document missing form type value'))
        except csv.Error, e:
            logger.error(e)
        # os.remove(path + '/' + x)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ingest-xbrl-rss-docs')
    parser.add_argument("--year",  type=str, action='store')
    parser.add_argument("--month",  type=str, action='store')
    parser.add_argument("--config-path",
                        type=str, action='store',
                        default='/home/ubuntu/ernest/config.json')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    args = parser.parse_args()

    with open(args.config_path, 'rb') as inf:
        config = json.load(inf)

    client = Elasticsearch(
        [{"host": config['es']['host'], "port": config['es']['port']}])

    tags = generate_tags()
    dei_tags = generate_dei_tags()
    logger = LOGGER('xbrl_ingest', args.log_file).create_parent()
    year = str(args.year)
    month = str(args.month)
    ingest(year, month)
