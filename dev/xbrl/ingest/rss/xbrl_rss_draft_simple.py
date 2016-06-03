import feedparser
import os.path
import sys, getopt, time, socket, os, csv, re, json
import requests
import xml.etree.ElementTree as ET
import zipfile, zlib
import argparse
import subprocess
import itertools

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
# CLI

parser = argparse.ArgumentParser(description='ingest-xbrl-rss-docs')
parser.add_argument("--ingest",   action='store_true') 
parser.add_argument("--download",   action='store_true') 
parser.add_argument("--year",  type=str, action='store')
parser.add_argument("--month",  type=str, action='store')
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

def parse_r( year, month ):
    command     = 'Rscript'
    path2script = '/home/ubuntu/ernest/dev/xbrl/ingest/rss/xbrl_parse_min.R'
    args        = [year, month]
    cmd         = [command, path2script] + args
    x           = subprocess.call(cmd, universal_newlines=True)


def dei_tree( dei_frame ): 
    k = dei_frame
    k.sort()
    c = list(k for k,_ in itertools.groupby(k))
    dei_tree = {} 
    for i in c: 
        dei_tree[i[2]] = {
            'fact'      : i[4], 
            'from_date' : i[9],
            'to_date'   : i[10]
            }
    return dei_tree


def fact_tree( tag_frame ): 
    z    = tag_frame
    tree = {} 
    for i in z: 
        tree[i[2]] = {
            i[1] : { 
                'fact'     : i[4],
                'unit'     : i[3],
                'decimals' : i[5],
                'scale'    : i[6],
                'sign'     : i[7],
                'factId'   : i[8],
                'from_date': i[9],
                'to_date'  : i[10]
            }
        }
    return tree


def build_object( frame ): 
    out = []
    for c in range(0, len(frame)): 
        x = frame[c]
        if 'TextBlock' in x[2] or 'Axis' in x[1]: 
            pass
        else: 
            try: 
                if int(x[0]): 
                    if len(re.findall('-', x[10])) == 2: 
                        out.append(x)
                    else: 
                        pass
                else: 
                    pass
            except ValueError: 
                pass
                # ---
    return out


def ingest(year, month): 
    path   = '/home/ubuntu/xbrl/' + year + '/' + month + '/parsed_min'
    for x in os.listdir(path):
        try: 
            doc    = path + '/' + x
            f      = open(doc, 'rU') 
            reader = csv.reader(f)
            rows   = list(reader)
            entry  = {
                "link"          : x,
                "year"          : year,
                "month"         : month,
                "entity_info"   : {}, 
                "facts"         : {}
            }
            # --- define list inputs
            indices   = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13]
            frame     = []
            for i in range(1, len(rows)): 
                try: 
                    row = [rows[i][k] for k in indices]
                    frame.append(row)
                except: 
                    pass
                    # ---
            dei_frame = build_object([frame[i] for i in range(0, len(frame)) if 'dei_' in frame[i][2]])
            tag_frame = build_object([frame[i] for i in range(0, len(frame)) if 'us-gaap_' in frame[i][2]]) 
            # --- structure doc entity information
            entry['entity_info'] = dei_tree(dei_frame)
            # --- eliminate non 10-K / 10-Q docs
            if entry['entity_info']['dei_DocumentType']['fact'] in ('10-K', '10-Q'):
                entry['facts'] = fact_tree(tag_frame)
                try: 
                    client.index(index = 'xbrl_rss_test', \
                                 doc_type = 'filing', body = entry, id = x)
                    #sample_out.append(entry)
                except: 
                    print(' -- parsing exception -- ')
            else: 
                print(entry['entity_info']['dei_DocumentType'])
        except csv.Error, e: 
            print(e)


# __ run 


if args.download: 
    parse_r(args.year, args.month)

if args.ingest: 
    ingest(args.year, args.month)
    