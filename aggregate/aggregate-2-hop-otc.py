"""
    Computes {
        "otc_paths" : ... number of 2 hop paths to OTC companies ...
        "total_paths" : ... number of 2 hop paths ...
    }
"""

import json
import argparse
from collections import defaultdict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

parser = argparse.ArgumentParser(description='aggregate-2-hop-otc')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))

query = {
    "_source": ["issuerCik", "ownerCik", "__meta__.is_otc"]
}
client = Elasticsearch(host=config['es']['host'],
                       port=config['es']['port'])


def reduce_is_otc(otc_array):
    d = defaultdict(list)
    for key, value in otc_array:
        try:
            a = d[key]
        except KeyError:
            a = False

        d[key] = a or value
    return [(key, d[key]) for key in d]


def hash_join(table1, index1, table2, index2):
        h = defaultdict(list)
        # hash phase
        for s in table1:
            h[s[index1]].append(s)
        # join phase
        return [(s[0], (r[1], s[1])) for r in table2 for s in h[r[index2]]]


is_otc_raw = []
edge_rdd_raw = []
for doc in scan(client,
                index=config['ownership']['index'],
                doc_type=config['ownership']['_type'],
                query=query):
    is_otc_raw.append((doc['_source']['issuerCik'],
                       doc['_source']['__meta__']['is_otc']))
    edge_rdd_raw.append((doc['_source']['ownerCik'],
                         doc['_source']['issuerCik']))

is_otc = reduce_is_otc(is_otc_raw)

edge_rdd = list(set(edge_rdd_raw))

two_hop = [(x[1][1], x[1][0])
           for x in hash_join(edge_rdd, 0, edge_rdd, 0) if x[1][0] != x[1][1]]

otc = [x[1] for x in hash_join(two_hop, 0, is_otc, 0)]
collect = {}
for o in otc:
    try:
        temp = collect[o[1]]
    except KeyError:
        temp = {"otc_neighbors": {"otc_paths": 0, "total_paths": 0}}

    if o[0] is True:
        temp['otc_neighbors']['otc_paths'] += 1

    temp['otc_neighbors']['total_paths'] += 1

    collect[o[1]] = temp


def ret_actions(collect):
    for key in collect:
        collect[key]['cik'] = key.zfill(10)
        yield {
            "_op_type": "update",
            "_index": config['agg']['index'],
            "_type": config['agg']['_type'],
            "_id": key.zfill(10),
            "doc": collect[key],
            "doc_as_upsert": True
            }


for a, b in streaming_bulk(client, actions=ret_actions(collect)):
    print(a, b)
