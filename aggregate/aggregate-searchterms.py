#!/usr/bin/env python2.7

"""
    Aggregate terms that we use to search for companies
"""

import json
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan
from collections import OrderedDict


def compute(x):
    return list(_compute(x))


def _compute(x):
    for k, v in x.items():
        yield {"_op_type": "update",
               "_index": config['agg']['index'],
               "_type": config['agg']['_type'],
               "_id": k,
               "doc": {"searchterms": [k, v["name"]]},
               "doc_as_upsert": True
               }


parser = argparse.ArgumentParser(description='searchterms')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

with open(args.config_path, 'rb') as inf:
    config = json.load(inf)

client = Elasticsearch(host=config['es']['host'],
                       port=config['es']['port'])

od = OrderedDict()
for doc in scan(client,
                index=config['symbology']['index'],
                doc_type=config['symbology']['_type'],
                query={"_source": ["cik", "ticker", "name"]}):
    try:
        od[str(doc['_source']['cik']).zfill(10)] = doc['_source']
    except KeyError:
        client.delete(index=config['symbology']['index'],
                      doc_type=config['symbology']['_type'],
                      id=doc['_id'])

for a, b in parallel_bulk(client, compute(od)):
    print(a, b)
