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

# query = {
#   "query" : {
#       "filtered" : {
#           "filter" : {
#               "missing" : {
#                  "field" : "__meta__.match_attempted"
#               }
#           }
#       }
#   }
# }


query = {
    "query" : { 
        "match_all" : {} 
    }
}



INDEX         = 'otce_halts_test'
TYPE          = 'halt'
REF_INDEX     = 'unified_halts_test'
REF_TYPE      = 'suspension'

# -- 
# functions

def to_ref_date(date): 
    d = int(re.sub('\D', '', date)) 
    out_date = datetime.utcfromtimestamp(d / 1000).strftime('%Y-%m-%d')
    return out_date

def to_long_date(date): 
    d = int(re.sub('\D', '', date)) 
    out_date = datetime.utcfromtimestamp(d / 1000).strftime('%Y-%m-%d %H:%M:%S')
    return out_date


def build_out(ratio, score, hits, body, a, hit):
    if ratio >= 65 and score >= 1 and hits > 0: 
        out = {
                "_id"      : body["_id"],
                "_type"    : REF_TYPE,
                "_index"   : REF_INDEX,
                "_op_type" : "update",
                "doc"      : { 
                    "__meta__" : { 
                        'finra' : { 
                            'ticker'         : hit['SymbolName'],
                            'company'        : hit['CompanyName'],
                            'haltResumeID'   : hit['HaltResumeID'],
                            'haltID'         : hit['TradeHaltID'],
                            'haltReasonCode' : hit['HaltReasonCode'],
                            'marketCat'      : hit['MarketCategoryLookup'],
                            'dateHalted'     : to_long_date(hit['DateHalted']),
                            'dateLoaded'     : to_long_date(hit['LoadDate']),
                            'score'          : body['_score'],
                            'ratio'          : ratio,
                            'secHalt'        : True,
                            'matched'        : True
                            }
                        }
                    }
            }
    else: 
        out = {
            "_id"      : a['_id'],# str(to_long_date(hit['DateHalted'])) + '_' + hit['SymbolName'],
            "_type"    : REF_TYPE,
            "_index"   : REF_INDEX,
            "_op_type" : "index",
            "_source"      : { 
                "date"           : to_ref_date(hit['DateHalted']),
                "company"        : hit['SymbolName'],
                "link"           : None,
                "release_number" : None,
                "__meta__" : { 
                    'finra' : { 
                        'ticker'         : hit['SymbolName'],
                        'company'        : hit['CompanyName'],
                        'haltResumeID'   : hit['HaltResumeID'],
                        'haltID'         : hit['TradeHaltID'],
                        'haltReasonCode' : hit['HaltReasonCode'],
                        'marketCat'      : hit['MarketCategoryLookup'],
                        'dateHalted'     : to_long_date(hit['DateHalted']),
                        'dateLoaded'     : to_long_date(hit['LoadDate']),
                        'score'          : 0,
                        'ratio'          : ratio, 
                        'secHalt'        : True,
                        'matched'        : False
                    }
                }
            }
        }   
    return out


def run(query): 
    for a in scan(client, index = INDEX, query = query):
        hit = a['_source'] 
        if hit['IsSECRelatedHalt'] == "Yes" and hit['ActionDescription'] == 'Halt': 
            res = client.search(index = REF_INDEX, body = {
                "sort" : [
                    {
                        "_score" : {
                            "order" : "desc"
                            }
                        }],
                "query" : {
                    "bool" : { 
                        "must" : [
                            {
                                "match" : {
                                    "company" : hit['CompanyName']
                                    }
                                },
                            {
                                "match" : { 
                                    "date" : to_ref_date(hit['DateHalted'])
                                        }
                                    }
                                ]
                            }
                        }
                    })
            if res['hits']['total'] > 0:
                mtc          = res['hits']['hits'][0]['_source']
                sym_name     = hit['CompanyName'].lower().replace('common stock', '').replace('ordinary shares', '')
                halt_name    = mtc['company'].lower() 
                x            = fuzz.token_sort_ratio(sym_name, halt_name)
                y            = fuzz.ratio(sym_name, halt_name)
                body         = res['hits']['hits'][0]
                out          = build_out(x, body['_score'], 1, body, a, hit)
                out_log = {
                    "_id"      : a['_id'],
                    "_type"    : TYPE,
                    "_index"   : INDEX,
                    "_op_type" : "update",
                    "doc"      : { 
                        "__meta__" : { 
                            "match_attempted" : True,
                            "match_success"   : True
                        }
                    }
                }
            elif res['hits']['total'] == 0: 
                out = build_out(0, 0, 0, 0, a, hit)
                out_log = {
                    "_id"      : a['_id'],
                    "_type"    : TYPE,
                    "_index"   : INDEX,
                    "_op_type" : "update",
                    "doc"      : { 
                        "__meta__" : { 
                            "match_attempted" : True,
                            "match_success"   : False
                        }
                    }
                }
            
            if out: 
                yield out
            yield out_log
        
        else: 
            pass



def run2(query): 
    for a in scan(client, index = INDEX, query = query):
        hit = a['_source'] 
        if hit['IsSECRelatedHalt'] == "No" and hit['ActionDescription'] == 'Halt': 
            _id = a['_id']# str(to_long_date(hit['DateHalted'])) + '_' + hit['SymbolName']
            out = {
                "_id"      : _id,
                "_type"    : REF_TYPE,
                "_index"   : REF_INDEX,
                "_op_type" : "index",
                "_source"      : { 
                        "date"           : to_ref_date(hit['DateHalted']),
                        "company"        : hit['SymbolName'],
                        "link"           : None,
                        "release_number" : None,
                        "__meta__" : { 
                            'finra' : { 
                                'ticker'         : hit['SymbolName'],
                                'company'        : hit['CompanyName'],
                                'haltResumeID'   : hit['HaltResumeID'],
                                'haltID'         : hit['TradeHaltID'],
                                'haltReasonCode' : hit['HaltReasonCode'],
                                'marketCat'      : hit['MarketCategoryLookup'],
                                'dateHalted'     : to_long_date(hit['DateHalted']),
                                'dateLoaded'     : to_long_date(hit['LoadDate']),
                                'score'          : 0,
                                'ratio'          : 0,
                                'secHalt'        : False,
                                'matched'        : False
                            }
                        }
                    }
            }
            out_log = {
                "_id"      : a['_id'],
                "_type"    : TYPE,
                "_index"   : INDEX,
                "_op_type" : "update",
                "doc"      : { 
                    "__meta__" : { 
                        "match_attempted" : True,
                        "match_success"   : False
                    }
                }
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









