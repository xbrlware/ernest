#!/usr/bin/env python

'''
    Add single neighbor tag for owners and issuers to ownership index;
    tag enables hiding terminal nodes in front end

    ** Note **
    This runs prospectively using the --most-recent argument
'''

import json
import argparse
import findspark; findspark.init()

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan
from pyspark import SparkContext

sc = SparkContext(appName='enrich_terminal_nodes')

parser = argparse.ArgumentParser(description='add single neighbor tags')
parser.add_argument('--from-scratch', dest='from_scratch', action="store_true")
parser.add_argument('--most-recent', dest='most_recent', action="store_true")
parser.add_argument('--issuer', dest='issuer', action="store_true")
parser.add_argument('--owner', dest='owner', action="store_true")
parser.add_argument('--config-path',
                    type=str,
                    action='store',
                    default='../config.json')
args = parser.parse_args()

config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host': config['es']['host'],
    'port': config['es']['port']}
])


def issuerStruc(x):
    key = x[1]['issuerCik']
    val = x[1]['ownerCik']
    return (key, [val])


def ownerStruc(x):
    key = x[1]['ownerCik']
    val = x[1]['issuerCik']
    return (key, [val])


def buildQuery(val):
    val = '__meta__.' + val + '_has_one_neighbor'
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "filtered": {
                            "filter": {
                                "missing": {
                                    "field": val
                                }
                            }
                        }
                    },
                    {
                        "match": {
                            val: True
                        }
                    }
                ],
                "minimum_should_match": 1
            }
        }
    }
    return query

# --
# build rdd

rdd = sc.newAPIHadoopRDD(
    "org.elasticsearch.hadoop.mr.EsInputFormat",
    "org.apache.hadoop.io.NullWritable",
    "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={
        "es.nodes": config['es']['host'],
        "es.port": str(config['es']['port']),
        "es.resource": "%s/%s" % (
            config['ownership']['index'], config['ownership']['_type'])
    }
)

# -
# run and write to elasticsearch

query = {
    "query": {
        "match_all": {}
    }
}

if args.issuer:
    query_type = 'issuer'
    dfIssuer = rdd.map(issuerStruc).reduceByKey(lambda a, b: a + b).filter(
        lambda x: len(list(set(x[1]))) == 1).collect()
    ownerIssuer = [i[0] for i in dfIssuer]

elif args.owner:
    query_type = 'owner'
    dfOwners = rdd.map(ownerStruc).reduceByKey(lambda a, b: a + b).filter(
        lambda x: len(list(set(x[1]))) == 1).collect()
    ownerIssuer = [i[0] for i in dfOwners]


if args.from_scratch:
    query = buildQuery(query_type)
else:
    query = query

actions = []
i = 0
print('Updating {0} {1} records...'.format(len(ownerIssuer), query_type))

q = {"query": {
        "bool": {
            "must_not": {
                "match": {
                    "__meta__." + query_type + "_has_one_neighbor": True
                }
            },
            "must": {
                "terms": {
                }
            }
        }
    }
}

oi = [ownerIssuer[j: j + 1024] for j in range(0, len(ownerIssuer), 1024)]

for p in oi:
    q["query"]["bool"]["must"]["terms"][query_type + "Cik"] = p
    for person in scan(client, index=config['ownership']['index'], query=q):
        actions.append({
            "_op_type": "update",
            "_index": config['ownership']['index'],
            "_id": person['_id'],
            "_type": person['_type'],
            "doc": {"__meta__": {query_type + "_has_one_neighbor": True}}
        })
        i += 1

        if i > 500:
            for success, info in parallel_bulk(client, actions, chunk_size=510):
                if not success:
                    print('Failed ::', info)
                else:
                    print('Info ::', info)

            actions = []
            i = 0

for success, info in parallel_bulk(client, actions, chunk_size=510):
    if not success:
        print('Failed ::', info)
    else:
        print('Info ::', info)

f_query = {
    "query": {
        "bool": {
            "must_not": {
                "terms": {
                    query_type + 'Cik': ownerIssuer
                }
            },
            "must": {
                "match": {
                    "__meta__." + query_type + "_has_one_neighbor": True
                }
            }
        }
    }
}

for answer in scan(client, index=config['ownership']['index'], query=f_query):
    answer['_source']['__meta__'][query_type + '_has_one_neighbor'] = False
    res = client.index(
        index=config['ownership']['index'],
        doc_type=answer['_type'],
        body=answer['_source'],
        id=answer['_id']
    )
    print(res)
