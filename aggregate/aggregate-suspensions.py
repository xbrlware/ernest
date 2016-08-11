'''
    Aggregate trading suspensions information
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-suspensions.py')

# -- 
# CLI

parser = argparse.ArgumentParser(description='aggregate-suspensions')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
# config = json.load(open('../config.json'))

# --
# Connections

query = {
    "query" : {
        "filtered" : {
            "filter" : {
                "exists" : {
                    "field" : "__meta__.sym.cik"
                }
            }
        }
    }
}

rdd = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass = "org.apache.hadoop.io.NullWritable",
    valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf = {
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['suspension']['index'], config['suspension']['_type']),
        "es.query"    : json.dumps(query)
   }
)

# --
# Functions

def compute(x):
    x = list(x)
    for xx in x:
        del xx['__meta__']
    
    return sorted(x, key=lambda x: x['date'])

# --
# Run

rdd.map(lambda x: (x[1]['__meta__']['sym']['cik'], x[1]))\
    .groupByKey()\
    .mapValues(compute)\
    .map(lambda x: ('-', {
        "cik" : x[0], 
        "suspensions" : tuple(x[1]),
        "suspensions_stringified" : JSON.dumps(tuple(x[1])),
    }))\
    .mapValues(json.dumps)\
    .saveAsNewAPIHadoopFile(
        path = '-',
        outputFormatClass = 'org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass = 'org.apache.hadoop.io.NullWritable', 
        valueClass = 'org.elasticsearch.hadoop.mr.LinkedMapWritable', 
        conf = {
            'es.input.json'      : 'true',
            'es.nodes'           : config['es']['host'],
            'es.port'            : str(config['es']['port']),
            'es.resource'        : '%s/%s' % (config['agg']['index'], config['agg']['_type']),
            'es.mapping.id'      : 'cik',
            'es.write.operation' : 'upsert'
        }
    )

