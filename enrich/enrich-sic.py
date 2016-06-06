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
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
parser.add_argument("--lookup-path", type=str, action='store', default='../reference/sic_ref.p')
parser.add_argument("--index", type=str, action='store')
args=parser.parse_args()

config = json.load(open(args.config_path))
lookup = pickle.load(open(args.lookup_path))
client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout=60000)


# -- 
# define query

query = {
    "query" : {
        "filtered" : {
            "filter" : {
                "missing" : {
                    "field" : "__meta__.sic_lab"
                }
            }
        }
    }
}

for doc in scan(client, index = INDEX, query = query): 
    print doc['_id']
    
    client.index(
        index    = config[args.index]['index'], 
        doc_type = config[args.index]['_type'], 
        id       = doc['_id'],
        body     = {
            "__meta__" : {
                "sic_lab" : lookup.get(doc['_source']['sic'], None)
            }
        }
    )
