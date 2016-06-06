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

import datetime
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

def unzip( year, month ): 
        dr = ('/home/ubuntu/sec/' + year + '/' + month + '/')
        onlyfiles = [f for f in listdir(dr) if isfile(join(dr, f))]
        for f in onlyfiles: 
            try: 
                fh = open(dr + f, 'rb')
                z = zipfile.ZipFile(fh)
                drct = '/home/ubuntu/xbrl/' + year + '/' \
                    + month + '/' + f + '/'
                if not os.path.exists(drct):
                    os.makedirs(drct)
                for name in z.namelist():
                    z.extract(name, drct)
                fh.close()
            except: 
                print(f)


def downloadfile( sourceurl, targetfname ):
    mem_file = ""
    good_read = False
    xbrlfile = None
    if os.path.isfile( targetfname ):
        print( "Local copy already exists" )
        return True
    else:
        print( "Downloading:", sourceurl )
        try:
            xbrlfile = urlopen( sourceurl )
            try:
                mem_file = xbrlfile.read()
                good_read = True
            finally:
                xbrlfile.close()
        except HTTPError as e:
            print( "HTTP Error:", e.code )
        except URLError as e:
            print( "URL Error:", e.reason )
        except socket.timeout:
            print( "Socket Timeout Error" )
        except: 
            print('TimeoutError')
        if good_read:
            output = open( targetfname, 'wb' )
            output.write( mem_file )
            output.close()
        return good_read


def SECdownload( year, month ):
    root = None
    feedFile = None
    feedData = None
    good_read = False
    itemIndex = 0
    edgarFilingsFeed = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-' + str(year) + '-' + str(month).zfill(2) + '.xml'
    print( edgarFilingsFeed )
    if not os.path.exists( "/home/ubuntu/sec/" + str(year) ):
        os.makedirs( "/home/ubuntu/sec/" + str(year) )
    if not os.path.exists( "/home/ubuntu/sec/" + str(year) + '/' + str(month).zfill(2) ):
        os.makedirs( "/home/ubuntu/sec/" + str(year) + '/' + str(month).zfill(2) )
    target_dir = "/home/ubuntu/sec/" + str(year) + '/' + str(month).zfill(2) + '/'
    try:
        feedFile = urlopen( edgarFilingsFeed )
        try:
            feedData = feedFile.read()
            good_read = True
        finally:
            feedFile.close()
    except HTTPError as e:
        print( "HTTP Error:", e.code )
    except URLError as e:
        print( "URL Error:", e.reason )
    # except TimeoutError as e:
    #     print( "Timeout Error:", e.reason )
    except socket.timeout:
        print( "Socket Timeout Error" )
    except: 
        print('TimeoutError')
    if not good_read:
        print( "Unable to download RSS feed document for the month:", year, month )
        return
    # we have to unfortunately use both feedparser (for normal cases) and ET for old-style RSS feeds,
    # because feedparser cannot handle the case where multiple xbrlFiles are referenced without enclosure
    try:
        root = ET.fromstring(feedData)
    except ET.ParseError as perr:
        print( "XML Parser Error:", perr )
    feed = feedparser.parse( feedData )
    try:
        print( feed[ "channel" ][ "title" ] )
    except KeyError as e:
        print( "Key Error:", e )
    # Process RSS feed and walk through all items contained
    for item in feed.entries:
        print( item[ "summary" ], item[ "title" ], item[ "published" ] )
        try:
            # Identify ZIP file enclosure, if available
            enclosures = [ l for l in item[ "links" ] if l[ "rel" ] == "enclosure" ]
            if ( len( enclosures ) > 0 ):
                # ZIP file enclosure exists, so we can just download the ZIP file
                enclosure = enclosures[0]
                sourceurl = enclosure[ "href" ]
                cik = item[ "edgar_ciknumber" ]
                targetfname = target_dir+cik+'-'+sourceurl.split('/')[-1]
                retry_counter = 3
                while retry_counter > 0:
                    good_read = downloadfile( sourceurl, targetfname ) ## first f(x) call
                    if good_read:
                        break
                    else:
                        print( "Retrying:", retry_counter )
                        retry_counter -= 1
            else:
                # We need to manually download all XBRL files here and ZIP them ourselves...
                linkname = item[ "link" ].split('/')[-1]
                linkbase = os.path.splitext(linkname)[0]
                cik = item[ "edgar_ciknumber" ]
                zipfname = target_dir+cik+'-'+linkbase+"-xbrl.zip"
                if os.path.isfile( zipfname ):
                    print( "Local copy already exists" )
                else:
                    edgarNamespace = {'edgar': 'http://www.sec.gov/Archives/edgar'}
                    currentItem = list(root.iter( "item" ))[itemIndex]
                    xbrlFiling = currentItem.find( "edgar:xbrlFiling", edgarNamespace )
                    xbrlFilesItem = xbrlFiling.find( "edgar:xbrlFiles", edgarNamespace )
                    xbrlFiles = xbrlFilesItem.findall( "edgar:xbrlFile", edgarNamespace )
                    if not os.path.exists(  target_dir+"temp" ):
                        os.makedirs( target_dir+"temp" )
                    zf = zipfile.ZipFile( zipfname, "w" )
                    try:
                        for xf in xbrlFiles:
                            xfurl = xf.get( "{http://www.sec.gov/Archives/edgar}url" )
                            if xfurl.endswith( (".xml",".xsd") ):
                                targetfname = target_dir+"temp/"+xfurl.split('/')[-1]
                                retry_counter = 3
                                while retry_counter > 0:
                                    good_read = downloadfile( xfurl, targetfname ) ## second f(x) call
                                    if good_read:
                                        break
                                    else:
                                        print( "Retrying:", retry_counter )
                                        retry_counter -= 1
                                zf.write( targetfname, xfurl.split('/')[-1], zipfile.ZIP_DEFLATED )
                                os.remove( targetfname )
                    finally:
                        zf.close()
                        os.rmdir( target_dir+"temp" )
        except KeyError, KeyboardInterrupt:
            print( 'Error' )
        finally:
            print( "----------" )
        itemIndex += 1


def parse_r( year, month ):
    command     = 'Rscript'
    path2script = '/home/ubuntu/ernest/scrape/xbrl-parse.R'
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


def fact_list( tag_frame, entry ): 
    z    = tag_frame
    tree = {} 
    for i in z: 
        if len(i[4]) > 20: 
            pass
        else: 
            if i[10] != entry['entity_info']['dei_DocumentType']['to_date']: 
                pass
            else: 
                try: 
                    x = tree[i[2]]
                except: 
                    tree[i[2]] = {
                        "value"   : i[4],
                        "context" : i[1],
                        "from"    : i[9],
                        "to"      : i[10]
                    }
                x = tree[i[2]]
                if toDate(x["from"]) < toDate(i[9]): 
                    pass
                elif toDate(x["from"]) == toDate(i[9]) and len(x['context']) < len(i[1]): 
                    pass
                elif toDate(x["from"]) > toDate(i[9]) or (toDate(x["from"]) == toDate(i[9]) and len(x['context']) > len(i[1])):
                    tree[i[2]] = {
                        "value"   : i[4],
                        "context" : i[1],
                        "from"    : find_date( i[9] ),
                        "to"      : find_date( i[10] )
                    }
                else: 
                    pass
    return tree



def find_date( date ): 
    print(date)
    if date == "NA": 
        pass
    else: 
        o = re.compile("\d{4}-\d{2}-\d{2}")
        l = re.findall(o, date)[0]
    return l 


def toDate ( date ): 
    if date == "NA": 
        date = datetime.strptime("1900-01-01", "%Y-%m-%d")
    else: 
        date = datetime.strptime(find_date( date ), "%Y-%m-%d")
    return date


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
                section = entry['entity_info']
                out_entry          = entry
                out_entry['facts'] = fact_list(tag_frame, entry)
                try: 
                    client.index(index = 'ernest_xbrl_rss', \
                                 doc_type = 'filing', body = out_entry, id = x)
                except: 
                    print(' -- parsing exception -- ')
            else: 
                print(entry['entity_info']['dei_DocumentType'])
        except csv.Error, e: 
            print(e)


# __ run 


if args.download: 
    SECdownload(args.year, args.month)
    unzip( args.year, args.month )
    parse_r(args.year, args.month)

if args.ingest: 
    ingest(args.year, args.month)
