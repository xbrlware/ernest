import argparse
import time
import json
from pprint import pprint
import re
from re import sub
import datetime
from datetime import date, timedelta
import holidays
import dateutil.parser as parser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk


# -- 
# cli

parser = argparse.ArgumentParser()
parser.add_argument("--config-path",   type = str, action = 'store')
args = parser.parse_args()

# -- 
# config

config_path = args.config_path
config      = json.load(open(config_path))


# -- 
# es connections

client = Elasticsearch([{'host' : config['es']['host'], \
                         'port' : config['es']['port']}])


# --
# define query

query = { 
  "query" : { 
    "bool" : { 
      "must" : [
        {
          "terms" : { 
            "_enrich.status" : ["Large Accelerated Filer", "Accelerated Filer", "Smaller Reporting Company", "Non-accelerated Filer"]
                }   
        },
        {
          "query" : { 
          "filtered": {
              "filter": {
                "missing": {
                  "field": "_enrich.deadline"
                }
              }
            }
          }
        }
        ]
      } 
    }
}



# --
# functions


def add_delinquency(doc): 
    doc = doc['_source']
    if doc['form'] == "10-K": 
        doc = __flag_10K(doc)
    elif doc['form'] == "10-Q": 
        doc = __flag_10Q(doc)
        #
    return doc


def __flag_10K(doc): 
    r = [int(i) for i in doc['_enrich']['period'].split('-')]
    d = datetime.date(r[0], r[1], r[2])  
    if doc['_enrich']['status'] == 'Large Accelerated Filer': 
        dl = __find_day(d + timedelta(days = 60))
        doc['_enrich']['deadline'] = dl
    elif doc['_enrich']['status'] == 'Accelerated Filer': 
        dl = __find_day(d + timedelta(days = 75))
        doc['_enrich']['deadline'] = dl
    elif doc['_enrich']['status'] in ('Non-accelerated Filer', 'Smaller Reporting Company'): 
        dl = __find_day(d + timedelta(days = 90))
        doc['_enrich']['deadline'] = dl
        #
    return __is_late(doc)
  



def __flag_10Q(doc): 
    r = [int(i) for i in doc['_enrich']['period'].split('-')]
    d = datetime.date(r[0], r[1], r[2])  
    if doc['_enrich']['status'] in ('Large Accelerated Filer', 'Accelerated Filer'): 
        dl = __find_day(d + timedelta(days = 40))
        doc['_enrich']['deadline'] = dl
    elif doc['_enrich']['status'] in ('Non-accelerated Filer', 'Smaller Reporting Company'): 
        dl = __find_day(d + timedelta(days = 45))
        doc['_enrich']['deadline'] = dl
        #
    return __is_late(doc)




def __find_day(dl):
    us_holidays = holidays.US()
    if (dl.weekday() < 5) and (dl not in us_holidays): 
        dl = dl
    elif dl.weekday() == 5: 
        dl = dl + timedelta(days = 2)
        if dl in us_holidays: 
            dl = dl + timedelta(days = 1)
        else: 
            dl = dl
    elif dl.weekday() == 6: 
        dl = dl + timedelta(days = 1)
        if dl in us_holidays: 
            dl = dl + timedelta(days = 1)
        else: 
            dl = dl
    elif dl in us_holidays: 
        dl = dl + timedelta(days = 1)
        if dl.weekday() == 5: 
            dl = dl + timedelta(days = 2)
            if dl in us_holidays: 
                dl = dl + timedelta(days = 1)
            else: 
                dl = dl
        if dl.weekday() == 6: 
            l = dl + timedelta(days = 1)
            if dl in us_holidays: 
                dl = dl + timedelta(days = 1)
            else: 
                dl = dl
                #
    return dl



def __is_late(doc): 
    filed = [int(i) for i in doc['date'].split('-')]
    filed = datetime.date(filed[0], filed[1], filed[2])  
    doc['_enrich']['days_to_deadline'] = \
            (doc['_enrich']['deadline'] - filed).days
    doc['_enrich']['deadline'] = doc['_enrich']['deadline'].strftime("%Y-%m-%d")
    if doc['_enrich']['days_to_deadline'] < 0: 
        doc['_enrich']['is_late'] = True
    else: 
        doc['_enrich']['is_late'] = False
        #
    return doc 



# --
# run

for a in scan(client, index = config['delinquency']['index'], query = query): 
    client.index(index = config['delinquency']['index'], doc_type = config['delinquency']['_type'], body = add_delinquency(a), id = a["_id"])