'''
    Aggregate financials information
'''

import json
import argparse
import itertools

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-financials.py')

# -- 
# CLI

parser = argparse.ArgumentParser(description='financials')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
# config = json.load(open('../config.json'))

# --
# Connections

query = {
    "_source" : [
        "cik",
        "date",
        "form",
        "__meta__.financials"
    ],
    "query" : {
        "filtered" : {
            "filter" : {
                "exists" : {
                    "field" : "__meta__.financials" # Update to use "has_financials" field
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
        "es.resource" : "%s/%s" % (config['financials']['index'], config['financials']['_type']),
        "es.query"    : json.dumps(query)
   }
)

# --
# Functions

def extract(x):
    for k,v in x['__meta__']['financials'].iteritems():
        if v and (k != 'interpolated'): # If there are other non-value fields here, add them or get errors
            yield {
                "form" : x['form'],
                "date" : x['date'],
                "field" : k,
                "value" : v['value']
            }

# --
# Run

rdd.map(lambda x: (str(x[1]['cik']).zfill(10), x[1]))\
    .flatMapValues(extract)\
    .groupByKey()\
    .map(lambda x: ('-', {
        "cik" : x[0], 
        "financials" : tuple(x[1]),
        "financials_stringified" : json.dumps(tuple(x[1])),
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


