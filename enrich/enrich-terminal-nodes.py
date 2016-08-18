#!/usr/bin/env python

'''
    Add single neighbor tag for owners and issuers to ownership index; tag enables hiding terminal nodes in front end

    ** Note **
    This runs prospectively using the --most-recent argument 
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
from pyspark import SparkContext

# -- 
# CLI

parser = argparse.ArgumentParser(description='add single neighbor tags')
parser.add_argument('--from-scratch', dest='from_scratch', action="store_true")
parser.add_argument('--most-recent', dest='most_recent', action="store_true")
parser.add_argument('--issuer', dest='issuer', action="store_true")
parser.add_argument('--owner', dest='owner', action="store_true")
parser.add_argument('--config-path', type=str, action='store', default='../config.json')
args = parser.parse_args()

# --
# global vars

config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']}
])


# -- 
# define query
query = {
    "query" : { 
        "match_all" : {} 
    }
}

# -- 
# functions

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
        "query" : { 
            "bool" : { 
                "should" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "missing" : { 
                                    "field" : val
                                }
                            }
                        }
                    }, 
                    {
                        "match" : { 
                            val : True
                        }
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }
    return query


# -- 
# build rdd

rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['ownership']['index'], config['ownership']['_type']),
        "es.query"    : json.dumps(query)
   }
)


# -- 
# filter and aggregate

dfOwners  = rdd.map(ownerStruc).reduceByKey(lambda a, b: a + b)\
            .filter(lambda x: len(list(set(x[1]))) == 1).collect()

dfIssuer  = rdd.map(issuerStruc).reduceByKey(lambda a, b: a + b)\
            .filter(lambda x: len(list(set(x[1]))) == 1).collect()

issuers   = [i[0] for i in dfIssuer]
owners    = [i[0] for i in dfOwners]



# -- 
# run and write to elasticsearch

if args.issuer: 
    if args.from_scratch: 
        query = buildQuery('issuer')
    else: 
        query = query
    for a in scan(client, index = config['ownership']['index'], query = query):
        if a['_source']['issuerCik'] in issuers: 
            a['_source']["__meta__"][u'issuer_has_one_neighbor'] = True
        else: 
            a['_source']["__meta__"][u'issuer_has_one_neighbor'] = False
        client.index(index = config['ownership']['index'], doc_type = config['ownership']['_type'],\
                     body = a['_source'], id = a['_id'])
if args.owner: 
    if args.from_scratch: 
        query = buildQuery('owner')
    else: 
        query = query
    for a in scan(client, index = config['ownership']['index'], query = query):
        if a['_source']['ownerCik'] in owners: 
            a['_source']["__meta__"][u'owner_has_one_neighbor'] = True
        else: 
            a['_source']["__meta__"][u'owner_has_one_neighbor'] = False
        client.index(index = config['ownership']['index'], doc_type = config['ownership']['_type'],\
                     body = a['_source'], id = a['_id'])