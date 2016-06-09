'''
    Aggregate terms that we'd use to search for companies
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-searchterms.py')

# -- 
# CLI

parser = argparse.ArgumentParser(description='searchterms')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
# config = json.load(open('../config.json'))

# --
# Connections

rdd = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass = "org.apache.hadoop.io.NullWritable",
    valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf = {
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['symbology']['index'], config['symbology']['_type']),
        "es.query"    : json.dumps({"_source" : ["cik", "ticker", "name"]})
   }
)

# --
# Functions

def _compute(x):
    for xx in x:
        for v in xx.itervalues():
            if v:
                yield v

def compute(x):
    return sorted(list(set(list(_compute(x)))))

# --
# Run

rdd.map(lambda x: (str(x[1]['cik']).zfill(10), x[1]))\
    .groupByKey()\
    .mapValues(compute)\
    .map(lambda x: ('-', {"cik" : x[0], "searchterms" : tuple(x[1])}))\
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

