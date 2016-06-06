import json
import itertools
import math
import argparse
import urllib2
from urllib2 import urlopen

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

# -- 
# cli

parser = argparse.ArgumentParser(description='ingest_finra_docs')
parser.add_argument("--directory", type = str, action = 'store')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()


# -- 
# config

config_path = args.config_path
config      = json.load(open(config_path))


# --
# set vars 

if args.directory == 'otc': 
    url = 'http://otce.finra.org/Directories/DirectoriesJson?pgnum='
    INDEX  = config['otc_directory']['index']
    TYPE   = config['otc_directory']['_type']
elif args.directory == 'halts':
    url = 'http://otce.finra.org/TradeHaltsHistorical/TradeHaltsHistoricalJson?pgnum='
    INDEX  = config['otc_halts']['index']
    TYPE   = config['otc_halts']['_type']
elif args.directory == 'delinquency':
    url = 'http://otce.finra.org/DCList/DCListJson?pgnum='
    INDEX  = config['otc_delinquency']['index']
    TYPE   = config['otc_delinquency']['_type']
else: 
    print('-- must choose directory: otc, halts, delinuency --')
    raise


# --
# es connection

client   = Elasticsearch([{'host' : config['es']['host'], 'port' : config['es']['port']}])

# -- 
# functions

def ingest_directory(url, INDEX, TYPE):    
    x = json.load(urllib2.urlopen(url + str(1)))
    r = x['iTotalRecords']
    n = int(math.ceil(float(r) / 25))
    for i in range(0, n + 1): 
        x    = json.load(urllib2.urlopen(url + str(i)))
        out  = x['aaData']
        for x in out:
            if args.directory == 'halts': 
                _id = str(x['TradeHaltID']) + '_' + str(x['SecurityID'])
            else: 
                _id = str(x['SecurityID'])
            client.index(index=INDEX, doc_type=TYPE, body=x, id=_id) 


# --
# run

ingest_directory(url, INDEX, TYPE)