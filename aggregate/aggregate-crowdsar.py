#!/usr/bin/env python

"""
    Compute number of posts and sentiment over time
    ** Recomputes everything every day -- that is less than ideal, but not
       critical **
"""

import json
import argparse
import itertools
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk
from elasticsearch_dsl import A, Search

parser = argparse.ArgumentParser(description='aggregate-crowdsar')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch(host=config['es']['host'],
                       port=config['es']['port'])


def compute_timeseries(x):
    cik = x[0][0]
    y = []
    try:
        for t in x:
            y.append(
                (datetime.strptime(t[1][0],
                                   '%Y-%m-%d %H:%M:%S'
                                   ).strftime('%Y-%m-%d'),
                    t[1][1]))
    except TypeError:
        y.append((None, t[1][1]))
    z = sorted(y, key=lambda x: x[0])
    cs = []

    for k, g in itertools.groupby(z, key=lambda x: x[0]):
        g = list(g)
        cs.append({
            "date": k,
            "n_posts": len(g),
            "tri_pred_neg": sum([p[1]['neg'] for p in g]),
            "tri_pred_neut": sum([p[1]['neut'] for p in g]),
            "tri_pred_pos": sum([p[1]['pos'] for p in g]),
        })
    doc = {"cik": cik,
           "crowdsar": cs,
           "crowdsar_stringified":
           map(json.dumps, cs) if len(cs) > 0 else None
           }

    return {"_op_type": "update",
            "_index": config['agg']['index'],
            "_type": config['agg']['_type'],
            "_id": cik.zfill(10),
            "doc": doc,
            "doc_as_upsert": True
            }


def collect_all(cik):
    yield compute_timeseries([
        (cik,
         (doc['_source']['time'],
          doc['_source']['__meta__']['tri_pred']
          )
         ) for doc in scan(client,
                           index=config['crowdsar']['index'],
                           doc_type=config['crowdsar']['_type'],
                           query={"query": {
                               "match": {
                                   "__meta__.sym.cik": cik
                                   }
                               }}
                           )
         ])


a = A("terms", field='__meta__.sym.cik', size=50000000)
s = Search(using=client,
           index=config['crowdsar']['index'],
           doc_type=config['crowdsar']['_type'])
s.aggs.bucket('ciks', a)
d = s.execute()
ciks = [i['key'] for h in d.aggregations for i in h['buckets']]
for c in ciks:
    for a, b in streaming_bulk(client, actions=collect_all(c)):
        print(a, b)
