import re, json
import pickle 
import argparse
import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# --
# cli 

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type = str, action = 'store')
parser.add_argument("--lookup-path", type = str, action = 'store')
parser.add_argument("--index", type = str, action = 'store')
args = parser.parse_args()


# --
# config 

config_path = args.config_path
config      = json.load(open(config_path))


# --
# lookup 

lookup_path = args.lookup_path
lookup      = pickle.load(open(lookup_path))


# --
# es connection

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)


# -- 
# define query

query = {
  "query" : {
      "filtered" : {
          "filter" : {
              "missing" : {
                  "field" : "_enrich.status"
              }
          }
      }
  }
}


# -- 
# define global reference

afs_ref = {
    'LAF' : 'Large Accelerated Filer',
    'ACC' : 'Accelerated Filer',
    'SRA' : 'Accelerated Filer',   # could be Smaller reporting company also; not sure should be simple to find
    'NON' : 'Non-accelerated Filer',
    'SML' : 'Smaller Reporting Company'
}


# --
# functions

def parse_adsh ( body ): 
    acc = re.compile("\d{5,}-\d{2}-\d{3,}")
    val = re.findall(acc, body['url'])[0]
    return val


def get_status ( sub ):
  if len(sub['afs']) == 0: 
    status = None
  else: 
    regex  = re.compile('[^a-zA-Z]')
    key    = regex.sub('', sub['afs'])
    status = afs_ref[key]
  return status


def enrich_status( body ): 
    body['_enrich'] = {}
    acc       = parse_adsh( body )
    query     = {"query" :{"match" :{"_id" : acc}}}
    acc_match = []
    for doc in scan(client, index = "xbrl_submissions", query = query): 
        acc_match.append(doc)
        # --
    if len(acc_match) == 1: 
        sub = acc_match[0]['_source']
        body['_enrich']['status'] = get_status( sub )
        body['_enrich']['meta']   = 'matched_acc'
        # --
    elif len(acc_match) == 0: 
        cik       = body['cik'].zfill(10)
        r         = map(int, body['date'].split('-'))
        date      = datetime.date(r[0], r[1], r[2])  
        query     = {"query" :{"match" :{"cik" : cik}}}
        cik_match = []
        for doc in scan(client, index = "xbrl_submissions", query = query): 
            x      = doc['_source']['filed']
            p      = [int(x[:4]), int(x[4:6]), int(x[6:8])]
            s_date = datetime.date(p[0], p[1], p[2])  
            doc['_source']['date_dif'] = abs((s_date - date).days)
            cik_match.append(doc['_source'])
            # --
        if len(cik_match) == 0: 
          body['_enrich']['meta'] = 'no_available_match'
        elif len(cik_match) > 0: 
            out = sorted(cik_match, key=lambda k: k['date_dif']) 
            body['_enrich']['status'] = get_status( out[0] )
            body['_enrich']['meta']   = 'matched_cik'
    else: 
        print('-- query not functioning properly --')
        # -- 
    return body


# --
# run

for doc in scan(client, index = "e_delin_test", query = query): 
    client.index(
        index    = "e_delin_test", 
        doc_type = "entry", 
        id       = doc["_id"],
        body     = enrich_status( doc['_source'] )
    )
    print(doc['_id'])