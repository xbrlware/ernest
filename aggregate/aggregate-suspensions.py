#!/usr/bin/env python2.7

""" Aggregate trading suspensions information """

import argparse
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk
from operator import itemgetter


parser = argparse.ArgumentParser(description='aggregate-suspensions')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch(host=config['es']['host'],
                       port=config['es']['port'])
query = {
    "query": {
        "filtered": {
            "filter": {
                "exists": {
                    "field": "__meta__.sym.cik"
                }
            }
        }
    }
}


def compute(x):
    r = []
    for key in x:
        for ele in x[key]:
            del ele['__meta__']
        srtd = sorted(x[key], key=itemgetter('date'), reverse=True)
        t_obj = {
            "cik": key,
            "suspensions": srtd
        }
        if len(srtd) > 0:
            t_obj["suspensions_stringified"] = map(str, srtd)
        else:
            t_obj["suspensions_stringified"] = str(srtd)

        r.append({
            "_op_type": "update",
            "_index": config['agg']['index'],
            "_type": config['agg']['_type'],
            "_id": key.zfill(10),
            "doc": t_obj,
            "doc_as_upsert": True
            })

    return r


def stream(d):
    for ele in compute(d):
        yield(ele)


d = {}
for a in scan(client,
              index=config['suspension']['index'],
              doc_type=config['suspension']['_type'],
              query=query):
    try:
        d[a['_source']['__meta__']['sym']['cik']].append(a['_source'])
    except KeyError:
        d[a['_source']['__meta__']['sym']['cik']] = [a['_source']]

for a, b in streaming_bulk(client, actions=stream(d)):
    print(a, b)
