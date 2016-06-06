'''
    Compute instances of ticker, name and SIC changes for all CIKS
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='otc-neighbors')

# -- 
# CLI

parser = argparse.ArgumentParser(description='aggregate-graph')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
# config = json.load(open('../config.json'))

# --
# Connections

query = {
    "_source" : ["issuerCik", "ownerCik", "__meta__.is_otc"]
}

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

edge_rdd = rdd.map(lambda x: (x[1]["ownerCik"], x[1]["issuerCik"])).distinct()

otc_rdd = rdd\
    .map(lambda x: (x[1]["ownerCik"], x[1]["__meta__"]["is_otc"]))\
    .reduceByKey(lambda a,b: a or b)

neibs = edge_rdd.join(otc_rdd).map(lambda x: x[1])\
    .mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda a,b: (0 + a[0] + b[0], a[1] + b[1]))\
    .map(lambda x: ('-', {
        "cik" : x[0],
        "otc_neighbors" : {
            "otc_count"   : x[1][0] + 0,
            "total_count" : x[1][1] + 0,
        },
    }))

neibs.saveAsNewAPIHadoopFile(
        path = '-',
        outputFormatClass = 'org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass = 'org.apache.hadoop.io.NullWritable', 
        valueClass = 'org.elasticsearch.hadoop.mr.LinkedMapWritable', 
        conf = {
            'es.input.json'      : 'false',
            'es.nodes'           : config['es']['host'],
            'es.port'            : str(config['es']['port']),
            'es.resource'        : '%s/%s' % (config['agg']['index'], config['agg']['_type']),
            'es.mapping.id'      : 'cik',
            'es.write.operation' : 'upsert'
        }
    )
