import re
import csv
import json
import pickle 
import argparse

from datetime import datetime, date, timedelta
from dateutil.parser import parse as dateparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

# --
# cli 

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
parser.add_argument("--lookup-path", type=str, action='store', default='../reference/sic_ref.p')
parser.add_argument("--index", type=str, action='store', required=True)
parser.add_argument("--field-name", type=str, action='store', required=True)
args=parser.parse_args()

config = json.load(open(args.config_path))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout=60000)

lookup = client.search(index='ernest_otc_directory_cat', body={
  "size" : 0,
  "aggs": {
    "terms": {
      "terms": {
        "field": "SymbolName",
        "size": 1000000
      }
    }
  }
})['aggregations']['terms']['buckets']
lookup = set([l['key'].upper() for l in lookup])

# -- 
# define query
def gen():
    query = {
        "query" : {
            "filtered" : {
                "filter" : {
                    "missing" : {
                        "field" : "__meta__.is_otc"
                    }
                }
            }
        }
    }
    
    for doc in scan(client, index=config[args.index]['index'], query=query): 
        yield {
            "_index"  : config[args.index]['index'], 
            "_type"   : config[args.index]['_type'], 
            "_id"     : doc['_id'],
            "op_type" : "update",
            "body" : {
                "__meta__" : {
                    "is_otc" : doc['_source'][args.field_name].upper() in lookup
                }
            }
        }


for a,b in streaming_bulk(client, gen(), chunk_size=2500):
    print a, b
