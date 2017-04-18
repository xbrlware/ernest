#!/usr/bin/env python

"""
    Build or update delinquency index using period and filer-status
    information from xbrl aqfs submission documents.
    The fiscal period end date and filer-status are required to calculate the
    filing deadline for each submission.

    ** Note **
    This runs prospectively after new 10-K and 10-Q documents have been
    downloaded to the edgar_index_cat index. This script must be run twice
    with different arguments:
        --status: add filer status to documents in ernest_aq_forms
        --period: add fiscal period end date to documents in ernest_aq_forms
        --update: runs prospectively appending new data to the index
        --from-scratch: rebuilds the index from scratch
"""

import argparse
import datetime
import json
import re
import urllib2

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


def parse_adsh(body):
    acc = re.compile("\d{5,}-\d{2}-\d{3,}")
    return re.findall(acc, body['url'])[0]


def get_status(sub, afs_ref):
    if len(sub['afs']) == 0:
        status = None
    else:
        regex = re.compile('[^a-zA-Z]')
        key = regex.sub('', sub['afs'])
        status = afs_ref[key]
    return status


def get_period(x):
    return datetime.date(int(x[:4]), int(x[4:6]), int(x[6:8]))


def enrich_status(body, afs_ref):
    body['_enrich'] = {}
    acc = parse_adsh(body)
    query = {"query": {"match": {"_id": acc}}}
    acc_match = []
    for doc in scan(client, index="xbrl_submissions_cat", query=query):
        acc_match.append(doc)

    if len(acc_match) == 1:
        sub = acc_match[0]['_source']
        body['_enrich']['status'] = get_status(sub, afs_ref)
        body['_enrich']['meta'] = 'matched_acc'
        # --
    elif len(acc_match) == 0:
        cik = body['cik'].zfill(10)
        r = map(int, body['date'].split('-'))
        date = datetime.date(r[0], r[1], r[2])
        query = {"query": {"match": {"cik": cik}}}
        cik_match = []
        for doc in scan(client, index="xbrl_submissions_cat", query=query):
            m = doc['_source']
            s_date = get_period(m['filed'])
            m['date_dif'] = abs((s_date - date).days)
            cik_match.append(m)
            # --
        if len(cik_match) == 0:
            body['_enrich']['meta'] = 'no_available_match'
            body['_enrich']['status'] = None
        elif len(cik_match) > 0:
            out = sorted(cik_match, key=lambda k: k['date_dif'])
            body['_enrich']['status'] = get_status(out[0], afs_ref)
            body['_enrich']['meta'] = 'matched_cik'
    else:
        print('-- query not functioning properly --')
    return body


def enrich_deadline(body):
    path = url_to_path(body['url'])
    hd = download_parsed(path)
    try:
        period = re.compile('CONFORMED PERIOD OF REPORT')
        period = [i for i in hd if len(re.findall(period, i)) > 0]
        period = re.sub('\D', '', period[0])
        prd = period[:4] + '-' + period[4:6] + '-' + period[6:8]
        body['_enrich']['period'] = prd
        try:
            doc_count = re.compile('PUBLIC DOCUMENT COUNT')
            doc_count = [i for i in hd if len(re.findall(doc_count, i)) > 0]
            doc_count = int(re.sub('\D', '', doc_count[0]))
        except:
            body['_enrich']['doc_count'] = None
    except:
        body['_enrich']['period'] = None
    return body


def url_to_path(url):
    url = url.split("/")
    path = 'https://www.sec.gov/Archives/edgar/data/' \
        + url[2] + "/" + re.sub('\D', '', url[-1]) + "/" + url[-1]
    return path


def run_header(txt):
    txt = __import__('re').sub('\r', '', txt)
    hd = txt[txt.find('<SEC-HEADER>'):txt.find('<DOCUMENT>')]
    hd = filter(None, hd.split('\n'))
    return hd


def download(path):
    x = []
    try:
        foo = urllib2.urlopen(path)
        for i in foo:
            x.append(i)
    except:
        print('malformed url - {}'.format(path))
    return ''.join(x)


def download_parsed(path):
    x = download(path)
    return run_header(x)


def add_meta(body):
    body['download_try'] = True
    return body


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    parser.add_argument("--from-scratch",
                        action='store_true')
    parser.add_argument("--update",
                        action='store_true')
    parser.add_argument("--status",
                        action='store_true')
    parser.add_argument("--period",
                        action='store_true')
    args = parser.parse_args()

    config = json.load(open(args.config_path))
    client = Elasticsearch([{
        'host': config['es']['host'],
        'port': config['es']['port']
    }], timeout=60000)

    if args.status:
        if args.from_scratch:
            q = {
                "query": {
                    "terms": {
                        "form.cat": ["10-K", "10-Q"]
                    }
                }
            }
        elif args.update:
            q = {
                "query": {
                    "bool": {
                        "must": [{
                            "query": {
                                "filtered": {
                                    "filter": {
                                        "missing": {
                                            "field": "download_try"
                                        }
                                     }
                                }
                            }
                        },
                            {
                                "terms": {
                                    "form.cat": ["10-K", "10-Q"]
                                }
                            }]
                    }
                }
            }
    elif args.period:
        q = {
            "query": {
                "filtered": {
                    "filter": {
                        "missing": {
                            "field": "_enrich.period"
                        }
                    }
                }
            }
        }

    afs_ref = {
        'LAF': 'Large Accelerated Filer',
        'ACC': 'Accelerated Filer',
        'SRA': 'Accelerated Filer',
        'NON': 'Non-accelerated Filer',
        'SML': 'Smaller Reporting Company'
    }

    if args.status:
        for doc in scan(client, index=config['edgar_index']['index'], query=q):
            client.index(
                index=config['aq_forms_enrich']['index'],
                doc_type=config['aq_forms_enrich']['_type'],
                id=doc["_id"],
                body=enrich_status(doc['_source'], afs_ref)
            )
            client.index(
                index=config['edgar_index']['index'],
                doc_type=config['edgar_index']['_type'],
                id=doc["_id"],
                body=add_meta(doc['_source'])
            )
            print doc['_id']

    elif args.period:
        for doc in scan(client, index=config['aq_forms_enrich']['index'],
                        query=q):
            client.index(
                index=config['aq_forms_enrich']['index'],
                doc_type=config['aq_forms_enrich']['_type'],
                id=doc["_id"],
                body=enrich_deadline(doc['_source'])
            )
            print doc['_id']
