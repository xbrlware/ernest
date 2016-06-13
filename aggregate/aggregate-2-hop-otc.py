'''
    aggregate-otc-neighbors.py
    
    Compute
        - number of 2 hop paths to OTC companies
        - number of 2 hop paths
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='otc-neighbors')

# -- 
# CLI

parser = argparse.ArgumentParser(description='aggregate-2-hop-otc')
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

# --
# Compute number of 2 hop OTC companies

is_otc = rdd.map(lambda x: x[1])\
    .map(lambda x: (x['issuerCik'], x['__meta__']['is_otc']))\
    .reduceByKey(lambda a,b: a or b)

edge_rdd = rdd.map(lambda x: x[1]).map(lambda x: (x["ownerCik"], x["issuerCik"])).distinct()
two_hop  = edge_rdd.join(edge_rdd).map(lambda x: x[1]).filter(lambda x: x[0] != x[1])

otc_neighbors = two_hop.map(lambda x: (x[1], x[0]))\
    .join(is_otc)\
    .map(lambda x: x[1])\
    .mapValues(lambda x: (x, 1))\
    .reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))

otc_neighbors.map(lambda x: ('-', {
    "cik" : x[0],
    "otc_neighbors" : {
        "otc_paths"   : x[1][0] + 0,
        "total_paths" : x[1][1] + 0
    } 
})).saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass='org.apache.hadoop.io.NullWritable', 
        valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', 
        conf={
            'es.input.json'      : 'false',
            'es.nodes'           : config['es']['host'],
            'es.port'            : str(config['es']['port']),
            'es.resource'        : '%s/%s' % (config['agg']['index'], config['agg']['_type']),
            'es.mapping.id'      : 'cik',
            'es.write.operation' : 'upsert'
        }
    )
