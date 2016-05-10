import re
import csv
import json
import pickle 
import argparse

from datetime import datetime, date, timedelta
from dateutil.parser import parse as dateparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# --
# cli 

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type = str, action = 'store')
parser.add_argument("--lookup-path", type = str, action = 'store')
parser.add_argument("--index", type = str, action = 'store')
args = parser.parse_args()


# --
# config 

config_path = args.config_path
config      = json.load(open(config_path))


# --
# lookup 

lookup_path = args.lookup_path
lookup      = pickle.load(open(lookup_path))


# --
# es connection

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)


# -- 
# define query

query = {
  "query" : {
      "filtered" : {
          "filter" : {
              "missing" : {
                  "field" : "__meta__.sic_label"
              }
          }
      }
  }
}


# -- 
# function

def enrich_sic( body ):
    body['__meta__'] = {} 
    try: 
        body['__meta__']['sic_lab'] = lookup[body['sic']]
    except KeyError: 
        body['__meta__']['sic_lab'] = None
    return body


# --
# run

if args.index == 'symbology':
    INDEX    = config['symbology']['index']
    TYPE     = config['symbology']['_type']
elif args.index == 'ownership': 
    INDEX    = config['ownership']['index']
    TYPE     = config['ownership']['_type']


for doc in scan(client, index = config['symbology']['index'], query = query): 
    client.index(
        index    = INDEX, 
        doc_type = TYPE, 
        id       = doc["_id"],
        body     = enrich_sic( doc['_source'] )
    )
    print(doc['_id'])