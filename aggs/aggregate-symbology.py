'''
    Compute instances of ticker, name and SIC changes for all CIKS
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-symbology.py')

# -- 
# CLI

parser = argparse.ArgumentParser(description='aggregate-symbology')
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
        "es.resource" : "%s/%s" % (config['symbology']['index'], config['symbology']['_type']),
   }
)

# --
# Functions

def _changes(records, field):
    old_record = records[0]
    for new_record in records[1:]:
        if new_record[field] != old_record[field]:
            yield OrderedDict([
                ( "field"    , field ),
                ( "old_val"  , old_record[field] ),
                ( "new_val"  , new_record[field] ),
                ( "old_date" , old_record['max_date'] ),
                ( "new_date" , new_record['min_date'] ),
            ])
        
        old_record = new_record    

def all_changes(x):
    x = sorted(x, key=lambda x: x['min_date'])
    return itertools.chain(*[_changes(x, field) for field in ['name', 'sic', 'ticker']])

# --
# Run

rdd.map(lambda x: (x[1]['cik'], x[1]))\
    .groupByKey()\
    .mapValues(all_changes)\
    .map(lambda x: ('-', {"cik" : x[0], "symbology" : tuple(x[1])}))\
    .filter(lambda x: len(x[1]['symbology']) > 0)\
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

