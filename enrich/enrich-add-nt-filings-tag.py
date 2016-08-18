#!/usr/bin/env python

'''
    Add enrich tag for filings that have NT documents in the edgar index

'''

import re
import json
import argparse
import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan


# --
# CLI
parser = argparse.ArgumentParser(description='add_nt_enrich')
parser.add_argument('--from-scratch', dest='from_scratch', action="store_true")
parser.add_argument('--most-recent', dest='most_recent', action="store_true")
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

# -- 
# connections

config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)


# -- 
# define query

if args.from_scratch: 
    query = {
        "query" : { 
            "bool" : { 
                "must" : [
                    {
                        "terms" : { 
                            "form.cat" : ["NT 10-K", "NT 10-Q"]
                        }
                    },
                    {
                        "filtered" : { 
                            "filter" : { 
                                "exists" : { 
                                    "field" : "_enrich.period"
                                }
                            }
                        }
                    }
                ]
            }
        }
    }
elif args.most_recent: 
    query = { 
        "query" : { 
            "bool" : { 
                "must" : [
                    {
                        "terms" : { 
                            "form.cat" : ["NT 10-K", "NT 10-Q"]
                        }
                    },
                    {
                        "bool" : {
                            "should" : [
                                {
                                    "filtered" : { 
                                        "filter" : { 
                                            "missing" : { 
                                                "field" : "__meta__.match_attempted"
                                            }
                                        }
                                    }
                                }, 
                                {
                                    "match" : { 
                                        "__meta__.matched" : False
                                    }
                                }
                                ], 
                                "minimum_should_match" : 1 
                            }
                        }
                    ]
                } 
            }
        }

# -- 
# functions

def matchNT(doc): 
    query = { 
        "query" : { 
            "bool" : {
                "must" : [
                {
                    "match" : { 
                        "form.cat" : doc['form'].replace("NT ", "")
                    }
                },
                {
                    "match" : { 
                        "cik.cat" : doc['cik']
                    }
                },
                {
                    "match" : { 
                        "_enrich.period" : doc['_enrich']['period']
                    }
                }, 
                {
                    "filtered" : { 
                        "filter" : { 
                            "missing" : { 
                                "field" : "_enrich.NT_exists"
                            }
                        }
                    }
                }
                ]
            }
        }
    }
    hits = []
    for a in scan(client, index = 'ernest_aq_forms', query = query):
        hits.append(a) 
    if len(hits) == 0: 
        hit = None
    elif len(hits) > 0: 
        hit = list(sorted(hits, key=lambda k: k['_source']['date']))[0]
        hit['_source']['_enrich']['NT_exists'] = True
    return hit

# -- 
# run

for a in scan(client, index = 'ernest_nt_filings', query = query):
    hit = matchNT(a['_source'])
    a['_source']['__meta__'] = {}
    a['_source']['__meta__']['match_attempted'] = True
    if hit != None: 
        a['_source']['__meta__']['matched'] = True
        client.index(
            index    = 'aq_forms_dev', 
            doc_type = 'filing', 
            id       = hit["_id"],
            body     = hit['_source']
        )
        client.index(
            index    = 'ernest_nt_filings', 
            doc_type = 'entry', 
            id       = a["_id"],
            body     = a['_source']
        )
    elif hit == None: 
        a['_source']['__meta__']['matched'] = False
        client.index(
            index    = 'ernest_nt_filings', 
            doc_type = 'entry', 
            id       = a["_id"],
            body     = a['_source']
        )