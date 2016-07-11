'''
    Compute instances of ticker, name and SIC changes for all CIKS
'''

import json
import argparse
import itertools
from collections import OrderedDict

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-graph.py')

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
    "_source" : [
        "header.REPORTING-OWNER.OWNER-DATA.CIK",
        "header.ISSUER.COMPANY-DATA.CIK"
    ]
}

rdd = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass = "org.apache.hadoop.io.NullWritable",
    valueClass = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf = {
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['forms']['index'], config['forms']['_type']),
        "es.query"    : json.dumps(query)
   }
)

# --
# Functions

def _extract_issuer_cik(header):
    for issuer in header.get('ISSUER', []):
        for company_data in issuer.get('COMPANY-DATA', []):
            for cik in company_data.get('CIK', []):
                yield str(cik).zfill(10)

def _extract_owner_cik(header):
    for owner in header.get('REPORTING-OWNER', []):
        for owner_data in owner.get('OWNER-DATA', []):
            for cik in owner_data.get('CIK', []):
                yield str(cik).zfill(10)

def extract(x):
    for issuer_cik in _extract_issuer_cik(x[1]['header']):
        for owner_cik in _extract_owner_cik(x[1]['header']):
            yield {
                "filing" : x[0],
                "issuer" : issuer_cik, 
                "owner"  : owner_cik,
                "is_otc" : {
                    "owner"  : False,
                    "issuer" : False,
                },
            }

# --
# Run

relats  = rdd.flatMap(extract)
owners  = relats.map(lambda x: (x['owner'], x))
issuers = relats.map(lambda x: (x['issuer'], x))

owners.fullOuterJoin(issuers).map(lambda x: ('-', {
    "cik"        : x[0],
    "was_issued" : x[1][0],
    "did_issue"  : x[1][1],
})).saveAsNewAPIHadoopFile(
        path = '-',
        outputFormatClass = 'org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass = 'org.apache.hadoop.io.NullWritable', 
        valueClass = 'org.elasticsearch.hadoop.mr.LinkedMapWritable', 
        conf = {
            'es.input.json'      : 'false',
            'es.nodes'           : config['es']['host'],
            'es.port'            : str(config['es']['port']),
            'es.resource'        : '%s/%s' % (config['network']['index'], config['network']['_type']),
            'es.mapping.id'      : 'cik',
            'es.write.operation' : 'upsert'
        }
    )

# How many shards is this going to create?

