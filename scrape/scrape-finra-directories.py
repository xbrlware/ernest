import json
import math
import argparse
import urllib2

from elasticsearch import Elasticsearch

# -- 
# cli

parser = argparse.ArgumentParser(description='ingest_finra_docs')
parser.add_argument("--directory", type=str, action='store')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{'host' : config['es']['host'], 'port' : config['es']['port']}])

urls = {
    "directory"   : 'http://otce.finra.org/Directories/DirectoriesJson?pgnum=',
    "halts"       : 'http://otce.finra.org/TradeHaltsHistorical/TradeHaltsHistoricalJson?pgnum=',
    "delinquency" : 'http://otce.finra.org/DCList/DCListJson?pgnum=',
}

# -- 
# functions

def ingest_directory(url, INDEX, TYPE):    
    x = json.load(urllib2.urlopen(url + str(1)))
    r = x['iTotalRecords']
    n = int(math.ceil(float(r) / 25))
    for i in range(0, n + 1): 
        x   = json.load(urllib2.urlopen(url + str(i)))
        out = x['aaData']
        for i in out:
            client.index(index=INDEX, doc_type=TYPE, body=i, id=i['SecurityID']) 

# --
# run

ingest_directory(
    urls[args.directory], 
    config['otc_%s' % args.directory]['index'], 
    config['otc_%s' % args.directory]['_type']
)