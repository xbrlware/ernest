'''
    Aggregate whether or not filings were delinquent
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-delinquency.py')

# -- 
# CLI

parser = argparse.ArgumentParser(description='delinquency')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))

# --
# Connections

rdd = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass = "org.apache.hadoop.io.NullWritable",
    valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf = {
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['delinquency']['index'], config['delinquency']['_type']),
   }
)

# --
# Functions

def _compute(x):
    return {
        "form"     : x["form"],
        "date"     : x["date"],
        "url"      : x["url"],
        "is_late"  : x["_enrich"].get("is_late"),
        "deadline" : x["_enrich"].get("deadline"),
        "period"   : x["_enrich"].get("period"),
    }

def compute(x):
    return map(_compute, x)

z = rdd.map(lambda x: (x[1]['cik'], x[1]))\
    .groupByKey()

# --
# Run

rdd.map(lambda x: (x[1]['cik'], x[1]))\
    .groupByKey()\
    .mapValues(compute)\
    .map(lambda x: ('-', {"cik" : x[0], "delinquency" : tuple(x[1])}))\
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

