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

# config_path = args.config_path
# config      = json.load(open(config_path))

config = json.load(open('/home/ubuntu/ernest/config.json')) 

# --
# global variables



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
                 "field" : "__meta__.match_attempted"
              }
          }
      }
  }
}



INDEX         = 'ernest_otc_halts_cat'
TYPE          = 'halt'
REF_INDEX     = 'unified_halts_test'
REF_TYPE      = 'suspension'

# -- 
# functions

def run(query): 
    for a in scan(client, index = INDEX, query = query):
        hit = a['_source'] 
        if hit['secHalt'] == True and hit['Action'] == 'Halt': 
            res = client.search(index = REF_INDEX, body = {
                "sort" : [{"_score" : {"order" : "desc"}}],
                "query" : {"bool" : { "must" : [{"match" : {"company" : hit['issuerName']}},
                {"match" : { "date" : hit['dateTime']}}]}}})
            if res['hits']['total'] > 0:
                doc = { 
                    "__meta__" : { 
                        'finra' : { 
                            'ticker'    : hit['ticker'],
                            'halt_code' : hit['haltCode'],
                            'market'    : hit['mktCtrOrigin'],
                            'score'     : res['hits']['hits'][0]['_score'],
                            'secHalt'   : True,
                            'matched'   : True
                            }
                        }
                    }
                doc2 = { 
                    "__meta__" : { 
                        "match_attempted" : True,
                        "match_success"   : True
                    }
                }
                out = {
                    "_id"      : res['hits']['hits'][0]['_id'],
                    "_type"    : REF_TYPE,
                    "_index"   : REF_INDEX,
                    "_op_type" : "update",
                    "doc"      : doc
                }
                out_log = {
                    "_id"      : a['_id'],
                    "_type"    : TYPE,
                    "_index"   : INDEX,
                    "_op_type" : "update",
                    "doc"      : doc2
                }
            elif res['hits']['total'] == 0: 
                doc2 = { 
                    "__meta__" : { 
                        "match_attempted" : True,
                        "match_success"   : True
                    }
                }
                out = {
                    "_id"      : hit['dateTime'] + '_' + hit['ticker'],
                    "_type"    : REF_TYPE,
                    "_index"   : REF_INDEX,
                    "_op_type" : "index",
                    "doc"      : { 
                        "date"           : hit['dateTime'],
                        "company"        : hit['issuerName'],
                        "link"           : None,
                        "release_number" : None,
                        '__meta__' : { 
                            'finra' : { 
                                'ticker'    : hit['ticker'],
                                'halt_code' : hit['haltCode'],
                                'market'    : hit['mktCtrOrigin'],
                                'score'     : 0,
                                'secHalt'   : False,
                                'matched'   : False
                            }
                        }
                    }
                }
                out_log = {
                    "_id"      : a['_id'],
                    "_type"    : TYPE,
                    "_index"   : INDEX,
                    "_op_type" : "update",
                    "doc"      : doc2
                }
            
            if out: 
                yield out
            yield out_log
        
        else: 
            pass



def run2(query): 
    for a in scan(client, index = INDEX, query = query):
        hit = a['_source'] 
        if hit['secHalt'] == False and hit['Action'] == 'Halt': 
            _id = hit['dateTime'] + '_' + hit['ticker']
            doc2 = { 
                "__meta__" : { 
                    "match_attempted" : True,
                    "match_success"   : False
                }
            }
            out = {
                "_id"      : _id,
                "_type"    : REF_TYPE,
                "_index"   : REF_INDEX,
                "_op_type" : "index",
                "doc"      : { 
                        "date"           : hit['dateTime'],
                        "company"        : hit['issuerName'],
                        "link"           : None,
                        "release_number" : None,
                        '__meta__' : { 
                            'finra' : { 
                                'ticker'    : hit['ticker'],
                                'halt_code' : hit['haltCode'],
                                'market'    : hit['mktCtrOrigin'],
                                'score'     : 0,
                                'secHalt'   : False,
                                'matched'   : False
                            }
                        }
                    }
            }
            out_log = {
                "_id"      : a['_id'],
                "_type"    : TYPE,
                "_index"   : INDEX,
                "_op_type" : "update",
                "doc"      : doc2
            }
            if out: 
                yield out
            yield out_log
        
        else: 
            pass


for a,b in streaming_bulk(client, run(query), chunk_size = 1000, raise_on_error = False):
    print a, b


for a,b in streaming_bulk(client, run2(query), chunk_size = 1000, raise_on_error = False):
    print a, b






