#!/usr/bin/env python

'''
    Coerce dates in performance graph to yyy-mm-dd from time string

    ** Note **
    This should be run after _run-all 

'''

import re
import csv
import json
import pickle 
import argparse

from datetime import datetime, date, timedelta
from dateutil.parser import parse as dateparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

# --
# cli 

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type = str, action = 'store', default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)


config = json.load(open('/home/ubuntu/ernest/config.json'))
client = Elasticsearch([{
    'host' : 'localhost', 
    'port' : 9205
}], timeout = 60000)

# -- 
# global vars 

INDEX = 'ernest_performance_graph'
TYPE  = 'excecution'


month_lookup = {
  'jan' : 1, 
  'feb' : 2,
  'mar' : 3,
  'apr' : 4, 
  'may' : 5, 
  'jun' : 6,
  'jul' : 7, 
  'aug' : 8,
  'sep' : 9, 
  'oct' : 10, 
  'nov' : 11, 
  'dec' : 12
}

# -- 
# define query

query = {
  "query" : {
      "filtered" : {
          "filter" : {
              "missing" : {
                 "field" : "_enrich.date_fix"
              }
          }
      }
  }
}


# -- 
# function

def enrich_dates(body):
    body['_enrich'] = {}
    body['_enrich']['date_fix']  = to_date(body['date']) 
    return body


def to_date(date): 
    d      = re.compile('\s{1}\d{1,2}\s{1}')
    d1     = re.findall(d, date)[0]
    d2     = re.sub('\D', '', d1).zfill(2)
    month  = date[:7][4:].lower()
    month2 = str(month_lookup[month])
    month3 = month2.zfill(2)
    year   = date[-4:]
    date2  = year + '-' + month3 + '-' + d2
    return date2

# --
# run

if __name__ == "__main__":
    for doc in scan(client, index = INDEX, query = query): 
        client.index(
            index    = INDEX, 
            doc_type = TYPE, 
            id       = doc["_id"],
            body     = enrich_dates( doc['_source'] )
        )
        print(doc['_id'])