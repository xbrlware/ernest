'''
    Copy index while increasing the number of shards
    (I feel like this is useful for Spark ETL)
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
parser.add_argument("--n-shards", type=int, default=20)
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

client.indices.create(index=args.target_index, body={
    "settings" : {
        "number_of_shards"   : args.n_shards,
        "number_of_replicas" : 0
    }
})

reindex(client, index, new_index, chunk_size=5000)