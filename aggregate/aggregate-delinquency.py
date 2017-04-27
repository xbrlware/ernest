#!/usr/bin/env python2.7

import argparse
import json
import itertools

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan


parser = argparse.ArgumentParser(description='delinquency')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
c = Elasticsearch(host='localhost', port=9205)
i = config['financials']['index']
d = config['financials']['_type']
q = {
    "_source": ["cik", "form", "date", "url", "_enrich"]
}


def _compute(x):
    y = []
    for ele in x[1]:
        y.append({
            "form": ele["form"],
            "date": ele["date"],
            "url": ele["url"],
            "is_late": ele["_enrich"].get("is_late"),
            "deadline": ele["_enrich"].get("deadline"),
            "period": ele["_enrich"].get("period"),
        })

    s = tuple(map(json.dumps, y)) if len(y) > 0 else None

    doc = {
        "cik": x[1][0]['cik'].zfill(10),
        "delinquency": y,
        "delinquency_stringified": json.dumps(s)
        }
    return {
        "_op_type": "update",
        "_index": config['agg']['index'],
        "_type": config['agg']['_type'],
        "_id": x[0].zfill(10),
        "doc": doc,
        "doc_as_upsert": True
        }


def compute(x):
    for ele in x:
        yield _compute(ele)


def group_by(r):
    return [(key, [v[1] for v in values])
            for key, values in itertools.groupby(r, lambda x: x[0])]


for a, b in streaming_bulk(c,
                           compute(
                            group_by(
                                [(doc['_source']['cik'].zfill(10),
                                  doc['_source'])
                                    for doc in scan(c,
                                                    index=i,
                                                    doc_type=d,
                                                    query=q)]
                                )
                            )):
    print(a, b)
