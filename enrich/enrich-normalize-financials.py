#!/usr/bin/env python

'''
    Scale year to date values in financials documents to quarter on quarter values

'''

import re
import json
import argparse
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from ftplib import FTP
import re
import json
import argparse
from hashlib import sha1
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# -- 
# CLI

parser = argparse.ArgumentParser(description='normalize_filings')
parser.add_argument('--from-scratch', dest='from_scratch', action="store_true")
parser.add_argument('--most-recent', dest='most_recent', action="store_true")
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()


# -- 
# connections

config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']}
])


# -- 
# query

if args.from_scratch: 
    query = { 
        "query" : { 
            "filtered" : { 
                "filter" : { 
                    "exists" : { 
                        "field" : "sub.fpID"
                    }
                }
            }
        }
    }
elif args.most_recent: 
    query = { 
        "query" : { 
            "bool" : { 
                "must" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "exists" : { 
                                    "field" : "sub.fpID"
                                }
                            }
                        }
                    },
                    {
                        "filtered" : { 
                            "filter" : { 
                                "missing" : { 
                                    "field" : "__meta__.financials.scaled"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }

# -- 
# functions

def toDatetime(x): 
    r    = map(int, x.split('-'))
    date = datetime.date(r[0], r[1], r[2])  
    return date

def normalize(x, pp):
    query = { 
        "query" : { 
            "bool" : { 
                "must" : [
                    {
                        "match" : { 
                            "cik" : x['cik']
                        }
                    },
                    {
                        "match" : { 
                            "sub.fpID" : pp
                        }
                    }
                ]
            }
        }
    }
    print(query)
    ref = []
    for i in scan(client, index = 'aq_forms_dev', query = query): 
        ref.append(i)
    if len(ref) == 1: 
        ref     = ref[0]
        docvals = x['__meta__']['financials']
        refvals = ref['_source']['__meta__']['financials']
        for k, v in docvals.iteritems():
            if type(v) == dict: 
                tag = str(k)
                try: 
                    if docvals[tag]['from'] != None: 
                        try: 
                            docvals[tag][u'norm'] = docvals[tag]['value'] - refvals[tag]['value']
                            docvals[tag][u'duration'] = (toDatetime(docvals[tag]['to']) - toDatetime(docvals[tag]['from'])).days / 30
                        except: 
                            docvals[tag][u'norm'] = None
                except: 
                    print('interpolated value')
            else: 
                pass
    else: 
        print('duplicate period assignment')
    x['__meta__']['financials']['scaled'] = True
    return x

def getPeriodRef(fp): 
    lkp = {'Q2' : 'Q1', 'Q3' : 'Q2', 'Q4' : 'Q3'}
    if 'Q1' in fp: 
        pass
    else: 
        c   = fp.split('__')
        pr  = lkp[c[0]]
        pid = pr + '__' + c[1]
        return pid

# -- 
# Run

for doc in scan(client, index = 'aq_forms_dev', query = query): 
    fp  = doc['_source']['sub']['fpID']
    try: 
        pp  = getPeriodRef(fp)
    except KeyError: 
        print('non-standard period id')
    if pp != None: 
        out = normalize(doc['_source'], pp)
    else:
        out = doc['_source']
    client.index(
        index    = 'aq_forms_dev', 
        doc_type = 'filing', 
        id       = doc["_id"],
        body     = out
    )