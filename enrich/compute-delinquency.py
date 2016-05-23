import json
import time
import argparse
import holidays
import dateutil.parser as parser

import datetime
from datetime import timedelta, date

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

deadlines = {
    "10-K" : {
        "Large Accelerated Filer"   : 60,
        "Accelerated Filer"         : 75,
        "Non-accelerated Filer"     : 90,
        "Smaller Reporting Company" : 90
    },
    "10-Q" : {
        "Large Accelerated Filer"   : 40,
        "Accelerated Filer"         : 40,
        "Non-accelerated Filer"     : 45,
        "Smaller Reporting Company" : 45
    }
}

# -- 
# CLI

parser = argparse.ArgumentParser()
parser.add_argument("--config-path",   type = str, action = 'store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{'host' : config['es']['host'], 'port' : config['es']['port']}])

# --
# Define query

query = { 
  "query" : { 
    "bool" : { 
      "must" : [ 
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
      },
      {
        "terms" : { 
          "_enrich.meta" : ["matched_cik", "matched_acc"]
                    }
              }
            ]
          }
      }
}

# --
# Functions

def add_delinquency(src, us_holidays=holidays.US()): 
    r = map(int, src['_enrich']['period'].split('-'))
    d = datetime.date(r[0], r[1], r[2])  
    
    dl = d + timedelta(days=deadlines[src['form']][src['_enrich']['status']])
    while (dl in us_holidays) or (dl.weekday() >= 5):
        dl += timedelta(days=1)
       
    filed = map(int, src['date'].split('-'))
    filed = datetime.date(filed[0], filed[1], filed[2])  
    src['_enrich']['deadline']         = dl.strftime("%Y-%m-%d")    
    src['_enrich']['days_to_deadline'] = (dl - filed).days
    src['_enrich']['is_late']          = src['_enrich']['days_to_deadline'] < 0
    
    return src


# --
# Run

for doc in scan(client, index = config['delinquency']['index'], query = query): 
    client.index(
        index    = config['delinquency']['index'], 
        doc_type = config['delinquency']['_type'], 
        id       = doc["_id"],
        body     = add_delinquency(doc['_source']), 
    )

