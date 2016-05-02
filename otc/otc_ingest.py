import os, csv, re, json
import zipfile, zlib
import argparse
import itertools

import urllib2 
from urllib2 import urlopen
from urllib2 import URLError
from urllib2 import HTTPError

from os import listdir
from os.path import isfile, join
from bs4 import BeautifulSoup

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

from datetime import datetime
from datetime import date, timedelta



# --
# cli 
parser = argparse.ArgumentParser(description='ingest_otc')
parser.add_argument("--from-scratch", action = 'store_true') 
parser.add_argument("--most-recent", action = 'store_true') 
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()


# -- 
# config
config_path = args.config_path
config      = json.load(open(config_path))


# --
# es 
client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])


# -- 
# functions

def ingest_raw(start_year):
    url   = 'http://otce.finra.org/DailyList/Archives'
    soup  = BeautifulSoup(urllib2.urlopen(url)) 
    links = soup.find("ul", {"class" : ['rawlist']}).findAll("li")
    # ---
    for link in links: 
        x     = link.find('a')
        _file = x['href']
        _text = x.get_text()
        base  = 'http://otce.finra.org'
        # --- 
        if 'txt' in str(_text): 
            o     = re.sub('\D', '', _text)
            month = int(o[:2])
            year  = int(o[4:])
            if year >= start_year:
                response  = urllib2.urlopen(base + _file)
                body      = [line.split('|') for line in response] 
                keys      = body[0]
                for p in range(1, len(body)):
                    vals            = body[p]
                    d               = dict(zip(keys, vals))
                    out = { 
                        'raw_source'        : 'txt_archive',
                        'source_doc'        : _text,
                        'DailyListDate'     : d['Daily List Date'],
                        'IssuerSymbol'      : d['New Symbol'],
                        'CompanyName'       : d['New Company Name'],
                        'Type'              : d['Type']
                    }
                    # ---
                    client.index(index = config['otc']['index'], doc_type = config['otc']['_type'], \
                        body = out)
            else: 
                pass
        elif 'zip' in str(_file): 
            year      = int(_text) 
            if year >= start_year: 
                response  = urllib2.urlopen(base + _file)       
                read      = response.read()
                with open('/home/ubuntu/data/otc_archives/' + _text + '.zip', 'w') as inf:
                    inf.write(read)
                    inf.close()
                    # ---
                with zipfile.ZipFile('/home/ubuntu/data/otc_archives/' + _text + '.zip', 'r') as z:
                    z.extractall('/home/ubuntu/data/otc_archives/')
                    # ---
                for i in os.listdir('/home/ubuntu/data/otc_archives/Daily_List_' + _text + '/'):
                    name      = str(i)
                    f         = open('/home/ubuntu/data/otc_archives/Daily_List_' + _text + '/'+ i, 'r')
                    x         = f.readlines()
                    body      = [line.split('|') for line  in x] 
                    body      = [[body[i][k].replace('\r\n', '') for k in range(len(body[i]))] for i in range(len(body))]
                    for m in range(1, len(body)): 
                        keys            = body[0]
                        vals            = body[m]
                        d               = dict(zip(keys, vals))
                        if name[:2] == 'BB':
                            out = { 
                            'raw_source'        : 'zip_archive',
                            'source_doc'        : name,
                            'DailyListDate'     : d['DailyListDate'],
                            'IssuerSymbol'      : d['NewSymbol'],
                            'CompanyName'       : d['NewName'],
                            'Type'              : d['Type']
                            }
                            # ---
                            client.index(index = config['otc']['index'], doc_type = config['otc']['_type'], \
                                body = out)
                        elif name[:2] == 'di':
                            out = { 
                                'raw_source'        : 'zip_archive',
                                'source_doc'        : name,
                                'DailyListDate'     : d['Daily List Date'],
                                'IssuerSymbol'      : d['Issue Symbol'],
                                'CompanyName'       : d['Company Name'],
                                'Type'              : 'dividend'
                            }
                            # ---
                            client.index(index = config['otc']['index'], doc_type = config['otc']['_type'], \
                                body = out)
            else: 
                pass



# -- 
# run


if args.from_scratch: 
    start_year = 2010
elif args.most_recent: 
    start_year = int(date.today().year)


ingest_raw(start_year)









