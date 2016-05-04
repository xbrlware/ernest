import re
import time
import json
import argparse
import urllib2
import xmltodict
from urllib2 import urlopen

from datetime import datetime, date, timedelta

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

from bs4 import BeautifulSoup
from ftplib import FTP

# -- 
# CLI

parser = argparse.ArgumentParser()
parser.add_argument("--config-path",   type = str, action = 'store')
parser.add_argument("--start-date",   type = str, action = 'store')
parser.add_argument("--end-date",   type = str, action = 'store')
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
                    "terms" : { 
                        "form" : ["10-K", "10-Q"]
                    }
                },
                {
                    "range" : { 
                        "date" : { 
                            "gte" : args.start_date,
                            "lte" : args.end_date
                        }
                    }
                }
            ]
        } 
    }
}


# -- 
# Functions

def get_link(r):    
    for i in r: 
        try: 
            link = i.find('a')['href']
            if len(re.findall("\d{2}.xml", link)): 
                return link
        except: 
            continue


def build_url(doc): 
    x = doc['_id'].split('/')
    
    url_params = (x[2], re.sub("\D", "", x[3]), x[3].replace('.txt', ''))
    url        = 'https://www.sec.gov/Archives/edgar/data/%s/%s/%s-index.htm' % url_params
    
    try:
        r = BeautifulSoup(urlopen(url)).find("table", {"summary" : ['Data Files']}).findAll('tr')
        return get_link(r)
    except:
        return '-- no link --'


def report_date(doc): 
    x = doc['_id'].split('/')
    
    url_params = (x[2], re.sub("\D", "", x[3]), x[3].replace('.txt', ''))
    url        = 'https://www.sec.gov/Archives/edgar/data/%s/%s/%s-index.htm' % url_params
    
    try: 
        soup = BeautifulSoup(urlopen(url)).find("div", {"class" : ['formContent']}).findAll("div", {"class" : ['formGrouping']})
        y = (list(list(soup)[1])[1]).get_text()
        x = (list(list(soup)[1])[3]).get_text()
        if y == 'Period of Report': 
            return x
        else: 
            return '-- no report date --'
    except: 
        return '-- no report date --'


def __enrich(doc): 
    body = doc['_source']
    body['_enrich'] = {}
    if build_url(doc) == '-- no link --': 
        body['_enrich']['status'] = None
    else: 
        try: 
            url  = 'https://www.sec.gov' + build_url(doc)
            soup = BeautifulSoup(urlopen(url))
            body['_enrich']['status'] = soup.find("dei:entityfilercategory").get_text() 
        except: 
            body['_enrich']['status'] = None
    
    if report_date(doc) != '-- no report date --': 
        body['_enrich']['period'] = report_date(doc)
    else: 
        try: 
            body['_enrich']['period'] = soup.find("dei:currentfiscalyearenddate").get_text()
        except: 
            try: 
                body['_enrich']['period'] = soup.find("dei:documentperiodenddate").get_text()
            except: 
                body['_enrich']['period'] = None
    
    return body


# --
# Run

for doc in scan(client, index=config['edgar_index']['index'], query=query): 
    try:
        _ = client.index(
            index    = config['delinquency']['index'], 
            doc_type = config['delinquency']['_type'], 
            body     = __enrich(doc), 
            id       = doc["_id"]
        )
    except KeyboardInterrupt: 
        raise
    except:
        print 'error at %s' % doc['_id']

