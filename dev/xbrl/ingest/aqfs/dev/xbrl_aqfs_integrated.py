import feedparser
import os.path
import sys, getopt, time, socket, os, csv, re, json
import requests
import xml.etree.ElementTree as ET
import zipfile, zlib
import argparse
import subprocess
import itertools
import pandas as pd 

import urllib2 
from urllib2 import urlopen
from urllib2 import URLError
from urllib2 import HTTPError

from os import listdir
from os.path import isfile, join
from collections import Counter
from bs4 import BeautifulSoup

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

from datetime import datetime
from datetime import date, timedelta

# --
# cli 

parser = argparse.ArgumentParser(description='grab_new_filings')
parser.add_argument("--from-scratch", action = 'store_true') 
parser.add_argument("--most-recent", action = 'store_true') 
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()


# -- 
# config

config_path = args.config_path
config      = json.load(open(config_path))


# --
# es connection

client = Elasticsearch([{
            'host' : config["es"]["host"], 
            'port' : config["es"]["port"]
        }], timeout = 60000)


# --
# functions 

def ingest_section( sec, period ): 
    if sec == 'sub': 
        end = 35
    else: 
        end = 8
        # ---
    f = open('/home/ubuntu/data/XBRL_AQFS/' + period + '/' + sec + '.txt', 'r')
    x = f.readlines()
    # - 
    name = 'lst_' + sec
    name = [] 
    for line in x: 
        row     = line.split('\t')
        row[end] = row[end].replace('\n', '')
        name.append(row)
        # -
    return name


def build_df( sec_list ): 
    sec = pd.DataFrame(sec_list)
    sec.columns = sec.iloc[0]
    sec         = sec[1:]
    return sec


def to_dict ( df ): 
    _dat  = [] 
    m     = df.values.tolist()
    keys  = list(df.columns.values)
    for i in range(0, len(m)): 
        x = keys
        y = m[i]
        dictionary = dict(zip(x, y))
        # - 
        _dat.append(dictionary)
        # - 
    return _dat


def run():
    periods = [] 
    if args.from_scratch: 
        for yr in range(2009, int(date.today().year) + 1): 
            if yr < date.today().year: 
                for qtr in range(1, 5): 
                    periods.append(str(yr) + 'q' + str(qtr))
                    #
            elif yr == date.today().year: 
                for qtr in range(1, (int(date.today().month) / 3) + 1): 
                    periods.append(str(yr) + 'q' + str(qtr))
    elif args.most_recent: 
        yr  = str(int(date.today().year)) 
        qtr = str(int(date.today().month) / 3) 
        periods.append(yr + 'q' + qtr)
        # ---
    for period in periods: 
        print('___ ingesting ___' + period)
        ingest(period)


def ingest ( period ):
    response        = urllib2.urlopen('https://www.sec.gov/data/financial-statements/' + str(period) + '.zip')
    aqfs            = response.read()
    # - 
    with open('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '.zip', 'w') as inf:
        inf.write(aqfs)
        inf.close()
        # - 
    with zipfile.ZipFile('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '.zip', 'r') as z:
        z.extractall('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '/')
        # - 
    num = build_df( ingest_section ( 'num', period ) )
    sub = build_df( ingest_section ( 'sub', period ) )
    pre = build_df( ingest_section ( 'pre', period ) )
    tag = build_df( ingest_section ( 'tag', period ) )
    # --
    numTag    = pd.merge(num, tag, on = ['tag', 'version'])
    numTagPre = pd.merge(numTag, pre, on = ['tag', 'adsh', 'version'])
    _dict     = to_dict ( numTagPre )
    _head     = to_dict ( self, sub )
    # -- 
    counter = 0
    for sub in _head: 
        counter += 1
        if counter % 100 == True:  
            print(counter)
        doc = {} 
        doc['submission'] = sub
        doc['facts']      = {} 
        facts = [_dict[i] for i in range(0, len(_dict)) \
                if _dict[i]['adsh']      == sub['adsh'] \
                and _dict[i]['ddate']    == sub['period'] \
                and _dict[i]['coreg']    == '' \
                and _dict[i]['abstract'] == '0' \
                and _dict[i]['custom']   == '0']
        for i in facts: 
            doc_core = { 
                'line'    : i['line'],
                'uom'     : i['uom'],
                'value'   : i['value'],
                'iord'    : i['iord'],
                'crdr'    : i['crdr'],
                'tlabel'  : i['tlabel'],
                'stmt'    : i['stmt'],
                'inpth'   : i['inpth'],
                'plabel'  : i['plabel']
            }
            try: 
                p = doc['facts'][i['tag']]
                try: 
                    p = doc['facts'][i['tag']][i['version']]
                    try: 
                        p = doc['facts'][i['tag']][i['version']][i['qtrs']]
                        try: 
                            p = doc['facts'][i['tag']][i['version']][i['qtrs']][i['report']]
                            try: 
                                p = doc['facts'][i['tag']][i['version']][i['qtrs']][i['report']][str(i['line']) + '_' + i['uom']]
                            except: 
                                doc['facts'][i['tag']][i['version']][i['qtrs']][i['report']][str(i['line']) + '_' + i['uom']] = doc_core
                        except: 
                            doc['facts'][i['tag']][i['version']][i['qtrs']][i['report']] = { 
                                str(i['line']) + '_' + i['uom']: doc_core
                                }
                    except:
                        doc['facts'][i['tag']][i['version']][i['qtrs']] = { 
                            i['report'] : { 
                                str(i['line']) + '_' + i['uom']: doc_core
                                }
                            }
                except: 
                    doc['facts'][i['tag']][i['version']] = { 
                        i['qtrs']: { 
                            i['report'] : { 
                                str(i['line']) + '_' + i['uom']: doc_core
                            }
                        }
                    }
            except: 
                doc['facts'][i['tag']] = {
                    i['version'] : { 
                        i['qtrs']: { 
                            i['report'] : { 
                                str(i['line']) + '_' + i['uom']: doc_core
                            }
                        }
                    } 
                }
                # --
        client.index(index = config['xbrl_aqfs']['index'], doc_type = config['xbrl_aqfs']['_type'], \
            body = doc, id = doc['submission']['adsh']) 


# --
# run

run() 



