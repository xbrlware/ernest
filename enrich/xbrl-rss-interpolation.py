import itertools

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from elasticsearch.helpers import reindex


# --
# CLI

parser = argparse.ArgumentParser(description='ingest-xbrl-rss-docs')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()


# -- 
# config 

config = json.load(open(args.config_path))


# -- 
# es connection

client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])



# -- 
# functions

def _assets( doc ): 
    val =((doc['liabilitiesAndStockholdersEquity'] if doc['liabilitiesAndStockholdersEquity'] != None else \
          (doc['liabilities'] if doc['liabilities'] != None else 0)) +
          (doc['stockholdersEquity'] if doc['stockholdersEquity'] != None else 0))
    return val


def _liabilities( doc ): 
    val =((doc['liabilitiesAndStockholdersEquity'] if doc['liabilitiesAndStockholdersEquity'] != None else \
          (doc['assets'] if doc['assets'] != None else 0)) -
          (doc['stockholdersEquity'] if doc['stockholdersEquity'] != None else 0))
    return val

def _stockholdersEquity( doc ): 
    val =((doc['liabilitiesAndStockholdersEquity'] if doc['liabilitiesAndStockholdersEquity'] != None else \
          (doc['assets'] if doc['assets'] != None else 0)) -
          (doc['liabilities'] if doc['liabilities'] != None else 0))
    return val

def _liabilitiesAndStockholdersEquity( doc ): 
    val =(doc['assets'] if doc['assets'] != None else \
         (doc['liabilities'] if doc['liabilities'] != None else 0) + \
         (doc['stockholdersEquity'] if doc['stockholdersEquity'] != None else 0))
    return val

def interpolate( a ): 
    doc = a['_source']['__meta__']['financials']
    for k, v in doc.iteritems(): 
        if v == None and k == 'assets': 
            doc['assets'] = _assets( doc )
        elif v == None and k == 'liabilities': 
            doc['liabilities'] = _liabilities( doc )
        elif v == None and k == 'stockholdersEquity': 
            doc['stockholdersEquity'] = _stockholdersEquity( doc )
        elif v == None and k == 'liabilitiesAndStockholdersEquity': 
            doc['liabilitiesAndStockholdersEquity'] = _liabilitiesAndStockholdersEquity( doc )        
        else: 
            pass
    doc['interpolated'] = True
    return a



# -- 
# run 


for a in scan(client, index = REF_INDEX, query = query): 
    try: 
        s = interpolate( a )
        client.index(index = 'crowdsar', doc_type = 'post', body = s['_source'], id = s['_id'])
    except: 
        pass

