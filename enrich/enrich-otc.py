'''
    Compute instances of ticker, name and SIC changes for all CIKS
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='enrich-otc.py')

# -- 
# CLI

parser = argparse.ArgumentParser(description='enrich-otc')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
# config = json.load(open('../config.json'))

# --
# Connections

rdd_otc = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass = "org.apache.hadoop.io.NullWritable",
    valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf = {
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['otc']['index'], config['otc']['_type']),
        "es.query"    : json.dumps({"_source" : "IssuerSymbol"})
   }
)


rdd_sym = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass = "org.apache.hadoop.io.NullWritable",
    valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf = {
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['symbology']['index'], config['symbology']['_type']),
        "es.query"    : json.dumps({"_source" : ["cik", "ticker"]})
   }
)

# --
# Run

rdd_otc = rdd_otc.map(lambda x: (x[1]['IssuerSymbol'], x[0])).filter(lambda x: x[0] != None)
rdd_sym = rdd_sym.map(lambda x: (x[1]['ticker'], x[1]['cik']))

rdd_otc.join(rdd_sym).map(lambda x: ('-', {
    "id"      : x[1][0],
    "_enrich" : { "cik" : x[1][1] }
})).saveAsNewAPIHadoopFile(
    path = '-',
    outputFormatClass = 'org.elasticsearch.hadoop.mr.EsOutputFormat',
    keyClass = 'org.apache.hadoop.io.NullWritable', 
    valueClass = 'org.elasticsearch.hadoop.mr.LinkedMapWritable', 
    conf = {
        'es.input.json'      : 'false',
        'es.nodes'           : config['es']['host'],
        'es.port'            : str(config['es']['port']),
        'es.resource'        : '%s/%s' % (config['otc']['index'], config['otc']['_type']),
        'es.mapping.id'      : 'id',
        'es.write.operation' : 'upsert'
    }
)

