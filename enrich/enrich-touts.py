#!/usr/bin/env python
'''
    Enrich touts with cik / ticker matching
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

parser = argparse.ArgumentParser(description='enrich_touts_cik_ticker')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
parser.add_argument("--from-scratch", action='store_true')
parser.add_argument("--most-recent", action='store_true')
args = parser.parse_args()

# -- 
# connections
config = json.load(open(args.config_path))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']}
])


# -- 
# query
if args.from_scratch: 
    query = { 
        "query" : { 
            "match_all" : {} 
        }
    }
elif args.most_recent: 
    query = { 
        "query" : { 
            "filtered" : { 
                "filter" : { 
                    "missing" : { 
                        "field" : "_enrich.match_attempted"
                    }
                }
            }
        }
    }

# -- 
# functions
def matchCIK(mens):
    ms = []
    for i in mens: 
        try: 
            i['_enrich'] = lkp[i['ticker']]
        except: 
            i['_enrich'] = {}
        i['_enrich']['match_attempted'] = True
        ms.append(i)
    return ms


def get_lookup():
    query = {
        "_source" : ["max_date", "sic", "cik", "ticker", "name"],
        "query" : {
            "filtered" : {
                "filter" : {
                    "exists" : {
                        "field" : "ticker"
                    }
                }
            }
        }
    }
    out = {}
    for a in scan(client, index=config['symbology']['index'], query=query):
        out[a['_source']['ticker']] = a['_source']
    return out


# -- 
# Run

lkp = get_lookup()

for a in scan(client, index = config['touts']['index'], query = query): 
    mens = a['_source']['mentions']
    if len(mens) > 0: 
        a['_source']['mentions'] = matchCIK(mens)
        client.index(
            index    = config['touts']['index'], 
            doc_type = config['touts']['_type'], 
            id       = a['_id'],
            body     = a['_source']
        )
    else: 
        pass
