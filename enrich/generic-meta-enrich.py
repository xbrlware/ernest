import json
import urllib2
import argparse
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


# -- 
# CLI

parser = argparse.ArgumentParser(description='Generic Monitoring Index')
parser.add_argument('--index', type=str, dest='index', action="store")
parser.add_argument('--type', type=str, dest='type', action="store")
parser.add_argument('--count-in', type=int, dest='count_in', action="store")
parser.add_argument('--count-out', type=str, dest='count_out', action="store")
parser.add_argument('--date', type=str, dest='date', action="store")
parser.add_argument('--config-path', type=str, action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([
    {"host" : config['es']['host'], 
    "port" : config['es']['port']}
], timeout=6000)


# --
# Functions

def buildOut(): 
    body = {
        "index" : args.index, 
        "count_in" : args.count_in, 
        "count_out" : args.count_out, 
        "date"      : args.date
    }
    return body



client.index(
    index    = args.index, 
    doc_type = args.type, 
    id       = args.index + "__" + args.date,
    body     = buildOut()
)