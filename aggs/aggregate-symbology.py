'''
    Compute instances of ticker, name and SIC changes for all CIKS
'''

import json
import argparse

# -- 
# CLI

parser = argparse.ArgumentParser(description='grab_new_filings')
parser.add_argument("--config-path", type=str, action='store')
args   = parser.parse_args()

config = json.load(open('/home/ubuntu/ernest/config.json'))

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

def _changes(z, field):
    


def all_changes(x):
    z = x[1]
    z = sorted(z, key=lambda x: x['min_date'])
    return _changes(z, 'name') + _changes(z, 'sic') + _changes(z, 'ticker')

# --
# Run

rdd.map(lambda x: (x[1]['cik'], x[1]))\
    .groupByKey()\
    .mapValues(all_changes)\
    .take(10)