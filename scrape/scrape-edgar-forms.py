#!/usr/bin/env python

'''
    Update ernest_forms_cat index with newly ingested edgar_index documents

    ** Note **
    This runs prospectively using a back-fill parameter only to grab docs
    that have not been tried or that have failed

'''

import re
import sys
import time
import json
import xmltodict
import argparse

import urllib2

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

from copy import copy
from threading import Timer, activeCount
from datetime import datetime
from datetime import date

T = time.time()


def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

parser = argparse.ArgumentParser(description='scrape-edgar-forms')
parser.add_argument("--back-fill", action='store_true')
parser.add_argument("--start-date", type=str, action='store', required=True)
parser.add_argument("--end-date",
                    type=str,
                    action='store',
                    default=date.today().strftime('%Y-%m-%d'))
parser.add_argument("--form-types",  type=str, action='store', required=True)
parser.add_argument("--section",     type=str, action='store', default='both')
parser.add_argument("--config-path",
                    type=str,
                    action='store',
                    default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))

HOSTNAME = config['es']['host']
HOSTPORT = config['es']['port']

FORMS_INDEX = config['forms']['index']
INDEX_INDEX = config['edgar_index']['index']

client = Elasticsearch([{'host': HOSTNAME, 'port': HOSTPORT}])

params = {
    'back_fill': args.back_fill,
    'start_date': datetime.strptime(args.start_date, '%Y-%m-%d'),
    'end_date': datetime.strptime(args.end_date, '%Y-%m-%d'),
    'form_types': map(int, args.form_types.split(',')),
    'section': args.section
}


docs = params['section'] in ['body', 'both']
header = params['section'] in ['header', 'both']
if (not docs) and (not header):
    raise Exception('section must be in [body, header, both]')

# Must be the right form type and between the dates
must = [{"terms": {"form.cat": params['form_types']}},
        {"range": {
            "date": {
                "gte": params['start_date'],
                "lte": params['end_date']
            }
        }
    }
]

# If not back filling, only load forms that haven't been tried
if not params['back_fill']:
    must.append({"filtered": {
        "filter": {
            "or": [
                {"missing": {"field": "download_try2"}},
                {"missing": {"field": "download_try_hdr"}},
            ]
        }
    }})

# Otherwise, try forms that haven't been tried or have failed
else:
    must.append({"bool": {
        "should": [
            {
                "filtered": {
                    "filter": {
                        "or": [
                            {"missing": {"field": "download_try2"}},
                            {"missing": {"field": "download_try_hdr"}}
                        ]
                    }
                }
            },
            {
                "bool": {
                    "must": [
                        {"match": {"download_success2": False}},
                        {"range": {"try_count_body": {"lte": 6}}}
                    ]
                }
            },
            {
                "bool": {
                    "must": [
                        {"match": {"download_success_hdr": False}},
                        {"range": {"try_count_hdr": {"lte": 6}}}
                    ]
                }
            }
        ],
        "minimum_should_match": 1
    }})


query = {
    "query": {
        "bool": {
            "must": must
        }
    }
}


def url_to_path(url, file_type):
    base = 'https://www.sec.gov/Archives/edgar/data/'
    url = url.split("/")
    n_sub = re.sub('\D', '', url[-1])
    if file_type == 'hdr':
        type_sub = re.sub('.txt', '.hdr.sgml', url[-1])
    else:
        type_sub = url[-1]

    return base + url[2] + "/" + n_sub + "/" + type_sub


def download(path):
    try:
        x = ''.join([i for i in urllib2.urlopen(path)])
    except:
        print >> sys.stderr, 'Error :: download :: %s' % (path)
        x = ''

    return x


def download_parsed(path):
    return run_header(download(path))


def run_header(txt):
    try:
        txt = re.sub('\r', '', txt)
        hd0 = txt[txt.find('<ACCESSION-NUMBER>'):txt.find('<DOCUMENT>')]
        hd1 = filter(None, hd0.split('\n'))
    except:
        print >> sys.stderr, 'Error :: run_header :: %s' % (txt)
        hd1 = ''

    return parse_header(hd1)


def parse_header(hd):
    curr = {}
    i = 0
    while i < len(hd):
        h = hd[i]
        if re.search('>.+', h) is not None:
            # Don't descend
            key = re.sub('<|(>.*)', '', h)
            val = re.sub('<[^>]*>', '', h)
            curr[key] = [val]
            i = i + 1
        else:
            if re.search('/', h) is None:
                key = re.sub('<|(>.*)', '', h)
                end = filter(lambda i: re.search('</' + h[1:], hd[i]),
                             range(i, len(hd)))
                tmp = curr.get(key, [])
                if len(end) > 0:
                    curr[key] = tmp + [parse_header(hd[(i + 1):(end[0])])]
                    i = end[0]
                else:
                    curr[key] = tmp + [None]
                    i = i + 1
            else:
                i = i + 1
    return curr


def get_headers(a, forms_index=FORMS_INDEX):
    path = url_to_path(a['_id'], 'hdr')
    out = {
        "_id": a['_id'],
        "_type": a['_type'],
        "_index": forms_index,
        "_op_type": 'update',
        "doc_as_upsert": True
    }
    out_log = {
        "_id": a['_id'],
        "_type": a['_type'],
        "_index": INDEX_INDEX,
        "_op_type": "update"
    }
    try:
        out['doc'] = {"header": download_parsed(path)}
        out_log['doc'] = {"download_try_hdr": True,
                          "download_success_hdr": True}
        return out, out_log
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        try:
            x = a['_source']['try_count_hdr']
        except:
            x = 0
        out_log['doc'] = {"download_try_hdr": True,
                          "download_success_hdr": False,
                          "try_count_hdr": x + 1}
        print 'failed @ %s' % path
        return None, out_log


def get_docs(a, forms_index=FORMS_INDEX):
    path = url_to_path(a['_id'], 'doc')
    out = {
        "_id": a['_id'],
        "_type": a['_type'],
        "_index": forms_index,
        "_op_type": "update",
        "doc_as_upsert": True
    }
    out_log = {
        "_id": a['_id'],
        "_type": a['_type'],
        "_index": INDEX_INDEX,
        "_op_type": "update"
    }
    try:
        page = download(path)
        split_string = 'ownershipDocument>'
        page = '<' + split_string + page.split(split_string)[1] + split_string
        page = re.sub('\n', '', page)
        page = re.sub(
            '>([0-9]{4}-[0-9]{2}-[0-9]{2})-[0-9]{2}:[0-9]{2}<', '>\\1<', page)
        page = re.sub('([0-9]{2})(- -)([0-9]{2})', '\\1-\\3', page)
        parsed_page = xmltodict.parse(page)
        out['doc'] = parsed_page
        out_log['doc'] = {"download_try2": True, "download_success2": True}
        return out, out_log
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        try:
            x = a['_source']['try_count_body']
            print('found try count body')
        except:
            x = 0

        print(x)
        out_log['doc'] = {"download_try2": True,
                          "download_success2": False,
                          "try_count_body": x + 1}
        print(out_log)
        print 'failed @ ' + path
        return None, out_log


def process_chunk(chunk, docs, header):
    for a in chunk:
        if docs:
            out, out_log = get_docs(a)
            if out:
                yield out
            yield out_log
        if header:
            out, out_log = get_headers(a)
            if out:
                yield out
            yield out_log


def load_chunk(chunk, docs, header):
    for a, b in streaming_bulk(client,
                               process_chunk(chunk, docs, header),
                               chunk_size=250):
        pass


def run(query, docs, header, chunk_size=1000, max_threads=5, counter=0):
    chunk = []
    for a in scan(client, index=INDEX_INDEX, query=query):
        chunk.append(a)
        if len(chunk) >= chunk_size:
            while activeCount() > max_threads:
                time.sleep(1)
            Timer(0, load_chunk, args=(copy(chunk), docs, header)).start()
            counter += len(chunk)
            print 'indexed %d in %f' % (counter, time.time() - T)
            chunk = []
    Timer(0, load_chunk, args=(copy(chunk), docs, header)).start()
    print 'done : %d' % counter

if __name__ == "__main__":
    run(query, docs, header)
