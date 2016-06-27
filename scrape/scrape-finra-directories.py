import json
import math
import argparse
import urllib2

from datetime import datetime, date, timedelta
from dateutil.parser import parse as dateparse

from elasticsearch import Elasticsearch

# -- 
# cli

parser = argparse.ArgumentParser(description='ingest_finra_docs')
parser.add_argument("--directory", type=str, action='store')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

# config = json.load(open(args.config_path))
config = json.load(open(config_path))
client = Elasticsearch([{'host' : config['es']['host'], 'port' : config['es']['port']}])

urls = {
    "directory"   : 'http://otce.finra.org/Directories/DirectoriesJson?pgnum=',
    "halts"       : 'http://otce.finra.org/TradeHaltsHistorical/TradeHaltsHistoricalJson?pgnum=',
    "delinquency" : 'http://otce.finra.org/DCList/DCListJson?pgnum='
}

# --
# functions

def get_max_date():
    global config 
    
    query = {
        "size" : 0,
        "aggs" : { "max" : { "max" : { "field" : "_enrich.halt_short_date" } } }
    }
    d = client.search(index = 'ernest_otce_halts_cat', body = query)
    x = int(d['aggregations']['max']['value'])
    max_date = datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%d')
    return max_date


def ingest_directory(url, INDEX, TYPE):    
    x = json.load(urllib2.urlopen(url + str(1)))
    r = x['iTotalRecords']
    n = int(math.ceil(float(r) / 25))
    for i in range(0, n + 1): 
        x   = json.load(urllib2.urlopen(url + str(i)))
        out = x['aaData']
        for i in out:
            if args.directory == 'halts':
                _id = str(i['HaltResumeID']) + '_' + str(i['SecurityID'])        
            else:       
                _id = str(i['SecurityID'])
            client.index(index=INDEX, doc_type=TYPE, body=i, id=_id) 

# --
# run

ingest_directory(
    urls[args.directory], 
    config['otc_%s' % args.directory]['index'], 
    config['otc_%s' % args.directory]['_type']
)