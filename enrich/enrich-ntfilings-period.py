#!/usr/bin/env python

'''
    Add period to NT filings documents in ernest_nt_filings 
    
'''

import re
import json
import argparse
import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

from ftplib import FTP
from sec_header_ftp_download import *

# --
# CLI

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)

ftpcon = SECFTP(FTP('ftp.sec.gov', 'anonymous'))

# -- 
# define query

query = {
    "query" : { 
        "filtered" : { 
            "filter" : { 
                "missing" : { 
                    "field" : "_enrich.period"
                }
            }
        }
    }
}


# --
# functions

def enrich_deadline(body): 
    path = ftpcon.url_to_path(body['url'])
    try: 
        x = ftpcon.download_parsed(path)['PERIOD'][0]
        prd = x[:4] + '-' + x[4:6] + '-' + x[6:8]
        body['_enrich'] = {} 
        body['_enrich']['period'] = prd
        try: 
            body['_enrich']['doc_count'] = int(ftpcon.download_parsed(path)['PUBLIC-DOCUMENT-COUNT'][0]) 
        except: 
            body['_enrich']['doc_count'] = None
    except: 
        body['_enrich'] = {} 
        body['_enrich']['period'] = None
    return body


# --
# run

for doc in scan(client, index='ernest_nt_filings', query=query): 
    client.index(
        index    = 'ernest_nt_filings', 
        doc_type = 'entry', 
        id       = doc["_id"],
        body     = enrich_deadline( doc['_source'] )
    )
    print doc['_id']
