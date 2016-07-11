#!/usr/bin/env python
'''
    Copy index while increasing the number of shards
'''

import sys
import json
import argparse
from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

# --
# CLI

parser = argparse.ArgumentParser(description='grab_new_filings')
parser.add_argument("--source-index", type=str)
parser.add_argument("--target-index", type=str)
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
parser.add_argument("--n-shards", type=int, default=9)
parser.add_argument("--n-replicas", type=int, default=1)
args = parser.parse_args()

config = json.load(open(args.config_path))

# --
# Connections 

client = Elasticsearch([{
    "host" : config['es']['host'],
    "port" : config['es']['port'],   
}])

old_index = config[args.source_index]['index']
doc_type  = config[args.source_index]['_type']

# --
# Run

try:
    client.indices.create(index=args.target_index, body={
        "settings" : {
            "number_of_shards"   : args.n_shards,
            "number_of_replicas" : args.n_replicas
        }
    })
except:
    print '!! INDEX ALREADY EXISTS !!'

print 'resharding %s to %s' % (old_index, args.target_index)
reindex(client, old_index, args.target_index, chunk_size=5000)
print '-- done --'
