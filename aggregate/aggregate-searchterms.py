#!/usr/bin/env python2.7

""" Aggregate terms that we use to search for companies """

import json
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan


def compute(x):
    return list(_compute(x))


def _compute(x):
    for k, v in x.items():
        d = {"searchterms": [k]}
        for ele in v:
            d["searchterms"].append(ele["name"])
            if ele["ticker"] is not None:
                d["searchterms"].append(ele["ticker"])

        d["searchterms"] = list(set(d["searchterms"]))
        yield {"_op_type": "update",
               "_index": config['agg']['index'],
               "_type": config['agg']['_type'],
               "_id": k,
               "doc": d,
               "doc_as_upsert": True
               }


parser = argparse.ArgumentParser(description='searchterms')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

with open(args.config_path, 'rb') as inf:
    config = json.load(inf)

client = Elasticsearch(host=config['es']['host'],
                       port=config['es']['port'])

od = dict()
for doc in scan(client,
                index=config['symbology']['index'],
                doc_type=config['symbology']['_type'],
                query={"_source": ["cik", "ticker", "name"]}):
    try:
        od[str(doc['_source']['cik']).zfill(10)].append(doc['_source'])
    except KeyError as e:
        try:
            od[str(doc['_source']['cik']).zfill(10)] = [doc['_source']]
        except KeyError as e2:
            print(e2, doc['_source'])

for a, b in parallel_bulk(client, compute(od)):
    print(a, b)
