from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

import requests
import re

from bs4 import BeautifulSoup

import urllib2
from urllib2 import urlopen

config = json.load(open('/home/ubuntu/ernest/config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout=60000)


HOSTNAME = config['es']['host']
HOSTPORT = config['es']['port']

FORMS_INDEX = 'ernest_10k_docs'
INDEX_INDEX = 'edgar_index_cat'


# start = datetime.strptime("2016-01-01", '%Y-%m-%d')

query = {
  "query" : { 
    "bool" : { 
      "must" :[
      {
        "match" : { 
          "form.cat" : "10-K"
        }
      },
      {
        "range" :{ 
          "date" : { 
            "gte" : "2013-01-01",
            "lte" : "2014-01-01"
            }
          }
      }
        ]
      }
    }
}



formVec = []

for a in scan(client, index=INDEX_INDEX, query=query):
    y   = re.compile('\d{1,}-\d{1,}-\d{1,}')
    acc = re.findall(y, a['_id'])[0]
    doc = {
        '_id' : a['_id'], 
        'cik' : a['_source']['cik'], 
        'acc' : acc,
        'accS': acc.replace('-', '')
    }
    doc['url'] = 'https://www.sec.gov/Archives/edgar/data/' + doc['cik'] + '/' + doc['accS'] + '/' + doc['acc'] + '-index.htm'
    formVec.append(doc)


# __ get text and build docs


errors = []

for i in formVec:
    try: 
        u    = i['url']
        url  = urlopen(u)
        soup = BeautifulSoup(url)
        tbl  = soup.find('table', {'class' : ['tableFile']}).findAll('tr')[1]
        tbl  = tbl.findAll('td')[2]
        link = tbl.find('a')['href']
        form = 'https://www.sec.gov' + link
        out  = requests.get(form)
        c    = BeautifulSoup(out.text).get_text(strip=True)
        c    = c.encode('ascii', 'replace')
        doc = {
            'doc'  : i['_id'], 
            'acc'  : i['acc'],
            'text' : c
        }
        client.index(
            index    = FORMS_INDEX,
            doc_type = 'textR', 
            id       = i['_id'], 
            body     = doc
        )
    except: 
        errors.append(i['url'])







