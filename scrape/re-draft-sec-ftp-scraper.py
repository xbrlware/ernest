#!/usr/bin/env python

'''
    Update ernest_forms_cat index with newly ingested edgar_index documents
    
    ** Note **
    This runs prospectively using a back-fill parameter only to grab docs 
    that have not been tried or that have failed 

'''

import re
import time
import json
import xmltodict
import argparse

import urllib2
from urllib2 import urlopen

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

from copy import copy
from threading import Timer, activeCount
from pprint import pprint
from ftplib import FTP
from datetime import datetime
from datetime import date, timedelta
# from sec_header_ftp_download import *

# --
# Global vars
T = time.time()

# --
# Helpers
def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

# --
# CLI
parser = argparse.ArgumentParser(description='scrape-edgar-forms')
parser.add_argument("--back-fill",   action='store_true') 
parser.add_argument("--start-date",  type=str, action='store', required=True)
parser.add_argument("--end-date",    type=str, action='store', default=date.today().strftime('%Y-%m-%d'))  
parser.add_argument("--form-types",  type=str, action='store', required=True)
parser.add_argument("--section",     type=str, action='store', default='both')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

# -- 
# Config
config = json.load(open('/home/ubuntu/ernest/config.json')) 

HOSTNAME = config['es']['host']
HOSTPORT = config['es']['port']

FORMS_INDEX = 'forms_index_test'
INDEX_INDEX = 'edgar_index_test'

# -- 
# IO
client = Elasticsearch([{'host' : HOSTNAME, 'port' : HOSTPORT}])

# --
# define query
# params = {
#     'back_fill'  : args.back_fill,
#     'start_date' : datetime.strptime(args.start_date, '%Y-%m-%d'),
#     'end_date'   : datetime.strptime(args.end_date, '%Y-%m-%d'),
#     'form_types' : map(int, args.form_types.split(',')),
#     'section'    : args.section
# }


# docs   = params['section'] in ['body', 'both']
# header = params['section'] in ['header', 'both']
# if (not docs) and (not header):
#     raise Exception('section must be in [body, header, both]')

docs = True
header = True
# Must be the right form type and between the dates
must = [
    {
        "terms" : { "form.cat" : ["3", "4"]}
    },
    {
        "range" : {
            "date" : {
                "gte" : "2017-01-01", 
                "lte" : "2017-01-17"
            }
        }
    }
]

# If not back filling, only load forms that haven't been tried
# if not params['back_fill']:
#     must.append({
#         "filtered" : {
#             "filter" : { 
#                 "or" : [
#                     {"missing" : { "field" : "download_try2"    }},
#                     {"missing" : { "field" : "download_try_hdr" }},
#                 ]
#             }
#         }
#     })

# must.append({
#     "filtered" : {
#         "filter" : { 
#             "or" : [
#                 {"missing" : { "field" : "download_try2"    }},
#                 {"missing" : { "field" : "download_try_hdr" }},
#             ]
#         }
#     }
# })


            
# Otherwise, try forms that haven't been tried or have failed
# else:

must.append({
    "bool" : { 
        "should" : [
            {
                "filtered" : {
                    "filter" : { 
                        "or" : [
                            {"missing" : { "field" : "download_try2" }},
                            {"missing" : { "field" : "download_try_hdr" }}
                        ]
                    }
                }
            },
            {
                "bool" : { 
                    "must" : [
                        {"match" : {"download_success2"    : False } }, 
                        {"range" : {"try_count_body" : {"lte" : 6}}}
                    ]
                }
            },
            {
                "bool" : { 
                    "must" : [
                        {"match" : {"download_success_hdr"    : False } }, 
                        {"range" : {"try_count_hdr" : {"lte" : 6}}}                           
                    ]
                }
            }
        ],
        "minimum_should_match" : 1
    }
})


query = {
    "query" : {
        "bool" : {
            "must" : must
        }
    }
}

pprint(query)

# ___ this query produced the correct 21604 number 

# {"query": {"bool": {"must": [{"terms": {"form.cat": ["3", "4"]}},
#                              {"range": {"date": {"gte": "2017-01-01",
#                                                  "lte": "2017-01-17"}}},
#                              {"filtered": {"filter": {"or": [{"missing": {"field": "download_try2"}},
#                                                              {"missing": {"field": "download_try_hdr"}}]}}}]}}}


# post edgar_index_test/_search
# {"query": {"bool": {"must": [{"terms": {"form.cat": ["3", "4"]}},
#                              {"range": {"date": {"gte": "2017-01-01",
#                                                  "lte": "2017-01-17"}}},
#                              {"filtered": {"filter": {"or": [{"missing": {"field": "download_try2"}},
#                                                              {"missing": {"field": "download_try_hdr"}}]}}}]}}}


# {'query': {'bool': {'must': [{'terms': {'form.cat': ['3', '4']}},
#                              {'range': {'date': {'gte': '2017-01-01',
#                                                  'lte': '2017-01-17'}}},
#                              {'bool': {'minimum_should_match': 1,
#                                        'should': [{'filtered': {'filter': {'or': [{'missing': {'field': 'download_try2'}},
#                                                                                   {'missing': {'field': 'download_try_hdr'}}]}}},
#                                                   {'bool': {'must': [{'match': {'download_success2': False}},
#                                                                      {'range': {'try_count_body': {'lte': 6}}}]}},
#                                                   {'bool': {'must': [{'match': {'download_success_hdr': False}},
#                                                                      {'range': {'try_count_hdr': {'lte': 6}}}]}}]}}]}}}


# indexed 6000 in 1010.563736
# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
#   File "<stdin>", line 3, in run
#   File "/usr/local/lib/python2.7/dist-packages/elasticsearch/helpers/__init__.py", line 285, in scan
#     resp = client.scroll(scroll_id, scroll=scroll)
#   File "/usr/local/lib/python2.7/dist-packages/elasticsearch/client/utils.py", line 69, in _wrapped
#     return func(*args, params=params, **kwargs)
#   File "/usr/local/lib/python2.7/dist-packages/elasticsearch/client/__init__.py", line 680, in scroll
#     params=params, body=body)
#   File "/usr/local/lib/python2.7/dist-packages/elasticsearch/transport.py", line 329, in perform_request
#     status, headers, data = connection.perform_request(method, url, params, body, ignore=ignore, timeout=timeout)
#   File "/usr/local/lib/python2.7/dist-packages/elasticsearch/connection/http_urllib3.py", line 109, in perform_request
#     self._raise_error(response.status, raw_data)
#   File "/usr/local/lib/python2.7/dist-packages/elasticsearch/connection/base.py", line 108, in _raise_error
#     raise HTTP_EXCEPTIONS.get(status_code, TransportError)(status_code, error_message, additional_info)
# elasticsearch.exceptions.NotFoundError: TransportError(404, u'{"_scroll_id":"c2NhbjswOzE7dG90YWxfaGl0czoyMTYwNDs=","took":5,"timed_out":false,
#     "_shards":{"total":5,"successful":0,"failed":5,"failures":[{"shard":-1,"index":null,"reason":{"type":"search_context_missing_exception",
#     "reason":"No search context found for id [935595]"}},{"shard":-1,"index":null,"reason":{"type":"search_context_missing_exception",
#     "reason":"No search context found for id [2204898]"}},{"shard":-1,"index":null,"reason":{"type":"search_context_missing_exception",
#     "reason":"No search context found for id [3199794]"}},{"shard":-1,"index":null,"reason":{"type":"search_context_missing_exception",
#     "reason":"No search context found for id [3878514]"}},{"shard":-1,"index":null,"reason":{"type":"search_context_missing_exception",
#     "reason":"No search context found for id [3878515]"}}]},"hits":{"total":21604,"max_score":0.0,"hits":[]}}')

# -- 
# Functions

def url_to_path(url, type):
    url = url.split("/")
    if type == 'hdr':
        path = 'https://www.sec.gov/Archives/edgar/data/'+ url[2] + "/" + re.sub('\D', '', url[-1]) + "/" + re.sub('.txt', '.hdr.sgml', url[-1])
    else: 
        path = 'https://www.sec.gov/Archives/edgar/data/'+ url[2] + "/" + re.sub('\D', '', url[-1]) + "/" + url[-1]
    return path

def download(path):
    foo  = urllib2.urlopen(path)
    x    = []
    for i in foo:
        x.append(i)
    return ''.join(x)

def download_parsed(path):
    x = download(path)
    return run_header(x)

def run_header(txt):
    txt = __import__('re').sub('\r', '', txt)
    hd  = txt[txt.find('<ACCESSION-NUMBER>'):txt.find('<DOCUMENT>')]
    hd  = filter(None, hd.split('\n'))
    return parse_header(hd)

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
                end = filter(lambda i:re.search('</' + h[1:], hd[i]), range(i, len(hd)))
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
    path = url_to_path(a['_id'], type = 'hdr')
    out = {
        "_id"           : a['_id'],
        "_type"         : a['_type'],
        "_index"        : forms_index,
        "_op_type"      : 'update',
        "doc_as_upsert" : True
    }
    out_log = {
        "_id"      : a['_id'],
        "_type"    : a['_type'], 
        "_index"   : INDEX_INDEX, 
        "_op_type" : "update"
    }
    try:
        out['doc'] = {"header" : download_parsed(path)}
        out_log['doc'] = {"download_try_hdr" : True, "download_success_hdr" : True}
        return out, out_log  
    except (KeyboardInterrupt, SystemExit):
        raise      
    except:
        try: 
            x = a['_source']['try_count_hdr']
        except: 
            x = 0
        out_log['doc'] = {"download_try_hdr" : True, \
                          "download_success_hdr" : False, \
                          "try_count_hdr" : x + 1}            
        print 'failed @ %s' % path
        return None, out_log




def get_docs(a, forms_index=FORMS_INDEX):
    path = url_to_path(a['_id'], type = 'doc')
    out = {
        "_id"           : a['_id'],
        "_type"         : a['_type'],
        "_index"        : forms_index,
        "_op_type"      : "update",
        "doc_as_upsert" : True
    }
    out_log = {
        "_id"      : a['_id'],
        "_type"    : a['_type'], 
        "_index"   : INDEX_INDEX, 
        "_op_type" : "update"
    }
    try:
        page         = download(path)
        split_string = 'ownershipDocument>'
        page         = '<' + split_string + page.split(split_string)[1] + split_string
        page         = re.sub('\n', '', page)
        page         = re.sub('>([0-9]{4}-[0-9]{2}-[0-9]{2})-[0-9]{2}:[0-9]{2}<', '>\\1<', page)
        page         = re.sub('([0-9]{2})(- -)([0-9]{2})', '\\1-\\3', page) 
        parsed_page  = xmltodict.parse(page)
        out['doc']   = parsed_page
        out_log['doc'] = {"download_try2" : True, "download_success2" : True}
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
        out_log['doc'] = {"download_try2" : True, \
                          "download_success2" : False, \
                          "try_count_body" : x + 1}
        print(out_log)
        print 'failed @ ' + path
        return None, out_log



# should change a['_id'] in this to path 

# def process_chunk(chunk, docs, header):
#     for a in chunk:
#         if docs:
#             out = get_docs(a)    
#             if out:
#                 yield out
#         if header:
#             out = get_headers(a)
#             if out: 
#                 yield out


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
    for a,b in streaming_bulk(client, process_chunk(chunk, docs, header), chunk_size=250):
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



run(query, docs, header)

# ___ some peculiarity here but will ocasionally fuck up

# ______ Scratch work utilities


# a = {
#     "_index": "edgar_index_test",
#     "_type": "entry",
#     "_id": "edgar/data/1450011/0001193125-17-008456.txt",
#     "_score": 1,
#     "_source": {
#       "cik": "1450011",
#       "date": "2017-01-12",
#       "url": "edgar/data/1450011/0001193125-17-008456.txt",
#       "name": "PIMCO ETF Trust",
#       "form": "485BXT"
#     }
#   }

