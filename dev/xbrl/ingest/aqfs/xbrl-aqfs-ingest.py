import feedparser
import os.path
import sys, getopt, time, socket, os, csv, re, json
import requests
import xml.etree.ElementTree as ET
import zipfile, zlib
import argparse
import subprocess
import itertools
import cPickle as pickle
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
parser.add_argument("--action", type=str, action = 'store')
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
    name = 'lst_' + sec
    name = [] 
    with open('/home/ubuntu/data/XBRL_AQFS/' + period + '/' + sec + '.txt') as infile:
        for line in infile:
            row      = line.split('\t')
            row[end] = row[end].replace('\n', '')
            name.append(row)
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


def download( period ): 
    response        = urllib2.urlopen('https://www.sec.gov/data/financial-statements/' + str(period) + '.zip')
    aqfs            = response.read()
    # - 
    with open('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '.zip', 'w') as inf:
        inf.write(aqfs)
        inf.close()
        # - 
    with zipfile.ZipFile('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '.zip', 'r') as z:
        z.extractall('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '/')
        os.remove('/home/ubuntu/data/XBRL_AQFS/' + str(period) + '.zip')


def run_download():
    periods = [] 
    if args.from_scratch:# args.from_scratch: 
        for yr in range(2009, int(date.today().year) + 1): 
            if yr < date.today().year: 
                for qtr in range(1, 5): 
                    periods.append(str(yr) + 'q' + str(qtr))
                    #
            elif yr == date.today().year: 
                for qtr in range(1, (int(date.today().month) / 3) + 1): 
                    periods.append(str(yr) + 'q' + str(qtr))
    elif args.most_recent:# args.most_recent: 
        yr  = str(int(date.today().year)) 
        qtr = str(int(date.today().month) / 3) 
        periods.append(yr + 'q' + qtr)
        # ---
    for period in periods: 
        if args.action == 'ingest': 
            print('___ ingesting aqfs ___' + period)
            run ( period )
        elif args.action == 'download': 
            print('___ downloading aqfs ___' + period)
            download ( period )
        else: 
            print('___ must chose one: download / ingest ___')


def build_objects( period, arg ): 
    if arg == 'sub':
        out = to_dict(build_df( ingest_section ( 'sub', period ) )) 
    elif arg == 'facts':
        num    = build_df( ingest_section ( 'num', period ) )
        pre    = build_df( ingest_section ( 'pre', period ) )
        tag    = build_df( ingest_section ( 'tag', period ) )
        numTag = pd.merge(num, tag, on = ['tag', 'version'])
        out    = pd.merge(numTag, pre, on = ['tag', 'adsh', 'version'])
    return out


def run( period ): 
    sub   = build_objects( period, 'sub' )
    facts = build_objects( period, 'facts' )
    for subr in sub: 
        ingest( period, subr, facts )


def u_decode( string ): 
    try: 
        string = string.decode('unicode escape').encode('ascii', 'ignore')
    except UnicodeDecodeError: 
        string = string.replace('\\', '')
        string = string.decode('unicode escape').encode('ascii', 'ignore')
    return string


def ingest ( period, subr, facts ):
    acc       = subr['adsh']
    _dict     = to_dict ( facts.loc[(facts.adsh == acc)] )
    doc       = {} 
    doc['submission'] = subr
    doc['facts']      = {} 
    facts = [_dict[i] for i in range(0, len(_dict)) \
            if  _dict[i]['ddate']    == subr['period'] \
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
        doc_core = {k: u_decode(v) for k, v in doc_core.items()}
        try: 
            p = doc['facts'][i['tag']]
            try: 
                p = doc['facts'][i['tag']][i['version']]
                try: 
                    p = doc['facts'][i['tag']][i['version']][i['qtrs'] + '_QTRS']
                    try: 
                        p = doc['facts'][i['tag']][i['version']][i['qtrs']+ '_QTRS']['RPT_' + i['report']]
                        try: 
                            p = doc['facts'][i['tag']][i['version']][i['qtrs']+ '_QTRS']['RPT_' + i['report']][str(i['line']) + '_' + i['uom']]
                        except: 
                            doc['facts'][i['tag']][i['version']][i['qtrs']+ '_QTRS']['RPT_' + i['report']][str(i['line']) + '_' + i['uom']] = doc_core
                    except: 
                        doc['facts'][i['tag']][i['version']][i['qtrs']+ '_QTRS']['RPT_' + i['report']] = { 
                            str(i['line']) + '_' + i['uom']: doc_core
                            }
                except:
                    doc['facts'][i['tag']][i['version']][i['qtrs']+ '_QTRS'] = { 
                        'RPT_' + i['report'] : { 
                            str(i['line']) + '_' + i['uom']: doc_core
                            }
                        }
            except: 
                doc['facts'][i['tag']][i['version']] = { 
                    i['qtrs'] + '_QTRS': { 
                        'RPT_' + i['report'] : { 
                            str(i['line']) + '_' + i['uom']: doc_core
                        }
                    }
                }
        except: 
            doc['facts'][i['tag']] = {
                i['version'] : { 
                    i['qtrs'] + '_QTRS': { 
                        'RPT_' + i['report'] : { 
                            str(i['line']) + '_' + i['uom']: doc_core
                        }
                    }
                } 
            }
            # --
    try:         
        client.index(index = config['xbrl_aqfs']['index'], doc_type = config['xbrl_aqfs']['_type'], \
            body = doc, id = doc['submission']['adsh']) 
        print(doc['submission']['adsh'])
    except UnicodeDecodeError, e: 
        print (' -- failed on doc ' + doc['submission']['adsh'])



# -- 
# run 

run_download()
