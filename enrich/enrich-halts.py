import re
import csv
import json
import pickle 
import argparse

from datetime import datetime, date, timedelta
from dateutil.parser import parse as dateparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# --
# cli 

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type = str, action = 'store', default='../config.json')
args = parser.parse_args()

# --
# config 

config_path = args.config_path
config      = json.load(open(config_path))

# --
# global variables

INDEX     = config['symbology']['index']
TYPE      = config['symbology']['_type']
REF_INDEX = config['suspension']['index']

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
                 "field" : "__meta__.halts"
              }
          }
      }
  }
}


# -- 
# functions

def run(query): 
    for a in scan(client, index = INDEX, query = query): 
        res = client.search(index = REF_INDEX, body = {
            "_source" : ["company", "date", "link"],
            "sort" : [
                {"_score" : {"order" : "desc"}}
            ],
            "query" : {
                "match" : {
                    "company" : a['_source']['name']
                    }
                }
            })
        if res['hits']['total'] > 0:
          mtc          = res['hits']['hits'][0]['_source']
          sym_name     = a['_source']['name'].lower()
          halt_name    = mtc['company'].lower() 
          x            = fuzz.token_sort_ratio(sym_name, halt_name)
          y            = fuzz.ratio(sym_name, halt_name)
          if res['hits']['hits'][0]['_score'] >= 1 and x >= 90):
              doc   = {
                  '__meta__' : {
                      'halts' : { 
                          "match_attempted"       : True, 
                          "company"               : mtc['company'], 
                          "date"                  : mtc['date'],
                          "link"                  : mtc['link'],
                          "fuzz_ratio"            : y,
                          "fuzz_token_sort_ratio" : x, 
                          "match_score"           : a['_score']
                      }
                  }
              }
          else: 
              doc = {
                  '__meta__' : {
                      'halts' : { 
                          "match_attempted" : True
                      }
                  }
              }
        else: 
            doc = {
                '__meta__' : {
                    'halts' : { 
                        "match_attempted" : True
                    }
                }
            }
        yield {
            "_id"      : a['_id'],
            "_type"    : TYPE,
            "_index"   : INDEX,
            "_op_type" : "update",
            "doc"      : doc
        }


# --
# run

for a,b in streaming_bulk(client, run(query), chunk_size = 1000, raise_on_error = False):
    print a, b