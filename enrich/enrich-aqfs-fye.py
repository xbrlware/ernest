#!/usr/bin/env python

'''
    Add fiscal period information to 10k and 10q filings in aq-forms index

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

parser = argparse.ArgumentParser(description='add_fye_info')
parser.add_argument('--sub', dest='sub', action="store_true")
parser.add_argument('--function', dest='function', action="store_true")
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()


# -- 
# connections

# config = json.load(open(args.config_path))
config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']}
])


# --
# query

query = { 
    "query" : { 
        "bool" : { 
            "must" : [
                {
                    "filtered" : { 
                        "filter" : { 
                            "exists" : { 
                                "field" : "_enrich.period"
                            }
                        }
                    }
                },
                {
                    "filtered" : { 
                        "filter" : { 
                            "missing" : { 
                                "field" : "sub"
                            }
                        }
                    }
                }, 
                {
                    "filtered" : { 
                        "filter" : { 
                            "exists" : { 
                                "field" : "__meta__.financials"
                            }
                        }
                    }
                }
            ]
        }
    }
}

query_func = { 
    "query" : { 
        "bool" : { 
            "must_not" : [
                {
                    "filtered" : { 
                        "filter" : { 
                            "missing" : { 
                                "field" : "_enrich.period"
                            }
                        }
                    }
                },
                {
                    "match" : { 
                        "sub.matched" : "acc_no"
                    }
                }, 
                {
                    "filtered" : { 
                        "filter" : { 
                            "missing" : { 
                                "field" : "__meta__.financials"
                            }
                        }
                    }
                }
            ]
        }
    }
}


# -- 
# functions --- Get quarter through period date math

def getFYE(period, fYEMonth, fYEDay):
    fyeTest = period[:5] + fYEMonth + '-' + fYEDay
    if toDatetime(period) > toDatetime(fyeTest): 
        fye = str(int(period[:4]) + 1) + '-' + fYEMonth + '-' + fYEDay
    elif toDatetime(period) <= (toDatetime(fyeTest)): 
        fye = fyeTest
    return fye

def toDatetime(x): 
    r    = map(int, x.split('-'))
    date = datetime.date(r[0], r[1], r[2])  
    return date

def addID(doc): 
    doc['sub'] = {}
    query = {
        "query" : { 
            "bool" : { 
                "must" : [
                    {
                        "match" : { 
                            "cik" : doc['cik']
                        }
                    }, 
                    { 
                        "range" : { 
                            "max_date" : { 
                                "gte" : doc['_enrich']['period']
                            }
                        }
                    },
                    {
                        "range" : { 
                            "min_date" : { 
                                "lte" : doc['_enrich']['period']
                            }
                        }
                    }
                ]
            }
        }
    }
    hit = []
    for a in scan(client, index = config['sub_agg']['index'], query = query): 
        hit.append(a)
    if len(hit) == 1: 
        ref        = hit[0]['_source']
        period     = doc['_enrich']['period']
        fye        = getFYE(period, ref['fYEMonth'], ref['fYEDay'])
        fp         = getFP(fye, period)
        doc['sub']['fiscalYearEnd'] = fye
        doc['sub']['fiscalPeriod']  = fp
        doc['sub']['matched'] = 'function'
        try: 
            doc['sub']['fpID'] = fp + '__' + fye[:4] 
        except: 
            doc['sub']['fpID'] = None
    else: 
        doc['sub']['fiscalYearEnd'] = None
    return doc


def getFP(fye, period): 
    delta = toDatetime(fye) - toDatetime(period)
    p     = round(float(delta.days) / float(30)) 
    if p in (8, 9, 10): 
        qtr = 'Q1'
    elif p in (5, 6, 7): 
        qtr = 'Q2'
    elif p == (2, 3, 4): 
        qtr = 'Q3'
    elif p == 0: 
        qtr = 'Q4'
    else: 
        qtr = None
    return qtr


# -- 
# functions -- Get quarter through submission acc no lookup

def parse_adsh(body): 
    acc = re.compile("\d{5,}-\d{2}-\d{3,}")
    val = re.findall(acc, body['url'])[0]
    return val

def addIDSub(doc): 
    default  = doc
    fye      = doc.get('sub', None)  
    if fye != None: 
        fye = x.get('fiscalYearEnd', None)
    doc['sub'] = {}
    acc        = parse_adsh( doc )
    query      = {
        "query" : {
            "bool" : { 
                "must_not" : [
                    {
                        "match" : { 
                            "fy.cat" : ""
                        }
                    }
                ], 
                "must" : [
                    {
                        "match" : { 
                            "adsh.cat" : acc 
                        }
                    }
                ]
            }
        }
    }
    hit = []
    for a in scan(client, index = config['xbrl_submissions']['index'], query = query): 
        hit.append(a)
    if len(hit) == 1: 
        ref        = hit[0]['_source']
        fp         = ref['fp']
        fy         = ref['fy']
        if fp == "FY": 
            fp = "Q4" 
        doc['sub']['fiscalYearEnd'] = fye
        doc['sub']['fiscalPeriod']  = fp
        doc['sub']['matched'] = 'acc_no'
        try: 
            doc['sub']['fpID'] = fp + '__' + fy 
        except: 
            doc['sub']['fpID'] == None
        return doc
    else: 
        return default


# -- 
# Run

if args.function: 
    for a in scan(client, index = config['aq_forms_enrich']['index'], query = query):
        try: 
            client.index(
                index    = config['aq_forms_enrich']['index'], 
                doc_type = config['aq_forms_enrich']['_type'], 
                id       = a["_id"],
                body     = addID( a['_source'] )
            )
        except: 
            print('failed on ' + a['_id'])


if args.sub: 
    for a in scan(client, index = config['aq_forms_enrich']['index'], query = query_func):
        try: 
            client.index(
                index    = config['aq_forms_enrich']['index'], 
                doc_type = config['aq_forms_enrich']['_type'], 
                id       = a["_id"],
                body     = addIDSub( a['_source'] )
            )
        except: 
            print('failed on ' + a['_id'])