import re
import os
import sys
import math
import json
import time
import urllib2
import argparse

from pprint import pprint
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# -- 
# CLI

parser = argparse.ArgumentParser(description = 'Scrape EDGAR indices')
parser.add_argument('--from-scratch', dest = 'from_scratch', action = "store_true")
parser.add_argument('--most-recent', dest = 'most_recent', action = "store_true")
parser.add_argument("--config-path", type = str, action = 'store')

args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])

# -- 
# update index with new docs

def get_max_date():
    global config 
    
    query = {
        "size" : 0,
        "aggs" : { "max" : { "max" : { "field" : "date" } } }
    }
    d = client.search(index = config['edgar_index']['index'], body = query)
    return int(d['aggregations']['max']['value'])


def download_index(yr, q, from_date = get_max_date()):
    global config
    parsing  = False 
    
    index_url = 'ftp://ftp.sec.gov/edgar/full-index/%d/QTR%d/master.idx' % (yr, q)
    for line in urllib2.urlopen(index_url):
        if parsing:
            cik, name, form, date, url = line.strip().split('|')
            date_int = 1000 * int(datetime.strptime(date, '%Y-%m-%d').strftime("%s"))
            if date_int > from_date: 
                yield {
                    "_id"     : url,
                    "_type"   : config['edgar_index']['_type'],
                    "_index"  : config['edgar_index']['index'],
                    "_source" : {
                        "cik"  : cik,
                        "name" : (name.replace("\\", '')).decode('unicode_escape'),
                        "form" : form,
                        "date" : date,
                        "url"  : url
                    }
                }
            else: 
                pass
        if line[0] == '-':
            parsing = True


# -- 
# run ingest

if args.most_recent:
    yr = date.today().year
    q  = date.today().month / 3 
    for a, b in streaming_bulk(client, download_index(yr, q), chunk_size = 1000):
        print a

elif args.from_scratch:
    yrs  = range(2011, int(date.today().year))
    qtrs = [1, 2, 3, 4]
    for yr in yrs:
        for qtr in qtrs:
            for a, b in streaming_bulk(client, download_index(yr, q, from_date = -1), chunk_size = 1000):
                print a
