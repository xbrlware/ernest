#!/usr/bin/env python

'''
    Update documents in ernest_nt_filings index

    Note:
        Runs prospectively using the most-recent argument

'''

import re
import json
import argparse
import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan


# --
# CLI
parser = argparse.ArgumentParser(description='add_nt_docs')
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
# query
if args.from_scratch: 
    query = { 
        "query" : { 
            "terms" : { 
                "form.cat" : ["NT 10-K", "NT 10-Q"]
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
                            "missing" : { 
                                "field" : "__meta__.migrated"
                            }
                        }
                    }
                },
                {
                    "terms" : { 
                        "form.cat" : ["NT 10-K", "NT 10-Q"]
                    }
                }
                ]
            }
        }
    }


# -- 
# Run

for a in scan(client, index = 'edgar_index_cat', query = query):
    a['_source']['__meta__']['migrated'] = True
    client.index(
        index    = 'ernest_nt_filings', 
        doc_type = 'entry', 
        id       = a['_id'],
        body     = a['_source']
    )