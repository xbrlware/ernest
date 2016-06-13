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
# cli 

parser = argparse.ArgumentParser(description='grab_new_filings')
parser.add_argument("--rss-year", type=str, action='store')
parser.add_argument("--rss-month", type=str, action='store')
parser.add_argument("--from-scratch", action = 'store_true') 
parser.add_argument("--most-recent", action = 'store_true') 
parser.add_argument("--config-path", type=str, action='store')
parser.add_argument("--class-opp", type=str, action='store')
args = parser.parse_args()


# -- 
# config
config_path = args.config_path
config      = json.load(open(config_path))


# --
# functions

class RSS: 
    def __init__(self):
        self.year  = args.rss_year
        self.month = args.rss_month.zfill(2)

    def unzip(self): 
        dr = ('/Users/culhane/Desktop/sec/' + self.year + '/' + self.month + '/')
        onlyfiles = [f for f in listdir(dr) if isfile(join(dr, f))]
        for f in onlyfiles: 
            try: 
                fh = open(dr + f, 'rb')
                z = zipfile.ZipFile(fh)
                drct = '/Users/culhane/xbrl/' + self.year + '/' \
                    + self.month + '/' + f + '/'
                if not os.path.exists(drct):
                    os.makedirs(drct)
                for name in z.namelist():
                    z.extract(name, drct)
                fh.close()
            except: 
                print(f)


    def downloadfile( self, sourceurl, targetfname ):
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


    def SECdownload( self ):
        root = None
        feedFile = None
        feedData = None
        good_read = False
        itemIndex = 0
        edgarFilingsFeed = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-' + str(self.year) + '-' + str(self.month).zfill(2) + '.xml'
        print( edgarFilingsFeed )
        if not os.path.exists( "sec/" + str(self.year) ):
            os.makedirs( "sec/" + str(self.year) )
        if not os.path.exists( "sec/" + str(self.year) + '/' + str(self.month).zfill(2) ):
            os.makedirs( "sec/" + str(self.year) + '/' + str(self.month).zfill(2) )
        target_dir = "sec/" + str(self.year) + '/' + str(self.month).zfill(2) + '/'
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
            print( "Unable to download RSS feed document for the month:", self.year, self.month )
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
                        good_read = self.downloadfile( sourceurl, targetfname ) ## first f(x) call
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
                                        good_read = self.downloadfile( xfurl, targetfname ) ## second f(x) call
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



class RSS_parse: 
    def __init__(self):
        self.year   = args.rss_year
        self.month  = args.rss_month.zfill(2)
        self.client = Elasticsearch([{
            'host' : 'localhost', #config["es"]["host"], 
            'port' : 9205 #config["es"]["port"]
        }], timeout = 60000)


    def parse_r(self):
        command     = 'Rscript'
        path2script = '/Users/culhane/Desktop/r_xbrl.R'
        args        = [self.year, self.month]
        cmd         = [command, path2script] + args
        x           = subprocess.call(cmd, universal_newlines=True)

    def test_order(self):
        print('DONE AFTER PARSING')


    def parse_sheets( self, out, sc ): 
        split    = sc.split('&&')
        _sheet   = split[0]
        _context = split[1]
        sheet   = [j for j in out if _sheet in j[1] and _context in j[6]]
        top_level = list(set([j[2] for j in sheet]) - set([j[3] for j in sheet]))
        tree      = {} 
        for k in top_level: 
            tree[k] = {} 
            sub     = [x[3] for x in sheet if x[2] == k]
            for s in sub: 
                z = [x[3] for x in sheet if x[2] == s]
                if len(z) == 0: 
                    fact    = [p[8] for p in sheet if p[3] == s]
                    decimal = [p[9] for p in sheet if p[3] == s]
                    balance = [p[10] for p in sheet if p[3] == s]
                    string  = [p[12] for p in sheet if p[3] == s]
                    # __ should add validation for lengths here
                    tree[k][s] = {
                        'fact'    : fact[0],
                        'decimal' : decimal[0],
                        'balance' : balance[0], 
                        'string'  : string[0]
                    } 
                elif len(z) != 0: 
                    tree[k][s] = {} 
                    for m in z: 
                        fact    = [p[8] for p in sheet if p[3] == m]
                        decimal = [p[9] for p in sheet if p[3] == m]
                        balance = [p[10] for p in sheet if p[3] == m]
                        string  = [p[12] for p in sheet if p[3] == m]
                        # __ should have length validation here too
                        tree[k][s][m] = {
                        'fact'    : fact[0],
                        'decimal' : decimal[0],
                        'balance' : balance[0], 
                        'string'  : string[0]
                        } 
        return tree


    def dei_tree( self, dei_frame ): 
        k = dei_frame
        k.sort()
        c = list(k for k,_ in itertools.groupby(k))
        dei_tree = {} 
        for i in c: 
            dei_tree[i[0]] = i[2]
        return dei_tree


    def build_object( self, tag_frame ): 
        out = []
        for c in range(0, len(tag_frame)): 
            x = tag_frame[c]
            if 'Axis' in x[0] or 'TextBlock' in x[0] or 'Axis' in x[6]: 
                pass
            else: 
                try: 
                    if int(x[5]): 
                        if '/role/label' in x[11]: 
                            out.append(x)
                        else: 
                            pass
                    else: 
                        pass
                except ValueError: 
                    pass
                    # ---
        return out


    def ingest( self ): 
        path   = '/home/ubuntu/xbrl/' + self.year + '/' + self.month + '/parsed'
        for x in os.listdir(path):
            try: 
                doc    = path + '/' + x
                # doc    = '/Users/culhane/Desktop/xbrl_test/test_docs/test_join_2.csv'
                f      = open(doc, 'rU') 
                reader = csv.reader(f)
                rows   = list(reader)
                entry  = {
                    "link" : x,
                    "year" : self.year,
                    "month": self.month,
                    "statements"   : {}, 
                    "entity_info"  : {}
                }
                # --- define list inputs
                indices   = [1, 2, 3, 4, 6, 13, 14, 15, 16, 17, 27, 30, 31]
                frame     = []
                for i in range(1, len(rows)): 
                    try: 
                        row = [rows[i][k] for k in indices]
                        frame.append(row)
                    except: 
                        pass
                        # ---
                dei_frame = [[frame[i][0], frame[i][6], frame[i][8]] for i in range(0, len(frame)) if 'dei_' in frame[i][0]]
                tag_frame = [frame[i] for i in range(0, len(frame)) if 'us-gaap_' in frame[i][0]] 
                # --- structure doc entity information
                entry['entity_info'] = self.dei_tree(dei_frame)
                # --- reduce and structure
                if entry['entity_info']['dei_DocumentType'] in ('10-K', '10-Q'):
                    out        = self.build_object(tag_frame)
                    __sections = list(set([i[1] + '&&' + i[6] for i in out]))
                    sections   = []
                    for i in __sections: 
                        x = re.findall('/role/.*&&.*', i)[0]
                        sections.append(x.replace('/role/', ''))
                        # ---
                    for sc in sections: 
                        c = sc.split('&&')
                        try:
                            p = entry['statements'][c[0]]
                            entry['statements'][c[0]][c[1]] = self.parse_sheets(out, sc)
                        except: 
                            entry['statements'][c[0]]       = {} 
                            entry['statements'][c[0]][c[1]] = self.parse_sheets(out, sc)
                    # --- index 
                    try: 
                        self.client.index(index = 'ernest_xbrl', doc_type = 'filing', body = entry, id = x)
                    except: 
                        print(' -- parsing exception -- ')
                else: 
                    print(entry['entity_info']['dei_DocumentType'])
            except csv.Error, e: 
                print(e)



class AQFS: 
    def __init__(self):
        self.year   = args.rss_year
        self.month  = args.rss_month.zfill(2)
        self.args   = parser.parse_args()
        self.client = Elasticsearch([{
            'host' : 'localhost', #config["es"]["host"], 
            'port' : 9205 #config["es"]["port"]
        }], timeout = 60000)


    def __ingest(period):
        response        = urllib2.urlopen('https://www.sec.gov/data/financial-statements/' + period + '.zip')
        aqfs            = response.read()
        # - 
        with open('/home/ubuntu/data/xbrl_aqfs/' + period + '.zip', 'w') as inf:
            inf.write(aqfs)
            inf.close()
            # - 
        with zipfile.ZipFile('/home/ubuntu/data/xbrl_aqfs/' + period + '.zip', 'r') as z:
            z.extractall('/home/ubuntu/data/xbrl_aqfs/' + period + '/')
            # - 
        f = open('/home/ubuntu/data/xbrl_aqfs/' + period + '/sub.txt', 'r')
        x = f.readlines()
        # - 
        lst = [] 
        for line in x: 
            row     = line.split('\t')
            row[35] = row[35].replace('\n', '')
            lst.append(row)
            # - 
        for i in range(1, len(lst)): 
            x = lst[0]
            y = lst[i]
            dictionary = dict(zip(x, y))
            dictionary['file_period'] = period
            # - 
            self.client.index(index = "xbrl_submissions", doc_type = 'filing', \
                body = dictionary, id = dictionary['adsh'])


    def run( self ):
        periods = [] 
        if self.args.from_scratch: 
            for yr in range(2009, int(date.today().year) + 1): 
                if yr < date.today().year: 
                    for qtr in range(1, 5): 
                        periods.append(str(yr) + 'q' + str(qtr))
                        #
                elif yr == date.today().year: 
                    for qtr in range(1, (int(date.today().month) / 3) + 1): 
                        periods.append(str(yr) + 'q' + str(qtr))
        elif self.args.most_recent: 
            yr  = str(int(date.today().year)) 
            qtr = str(int(date.today().month) / 3) 
            periods.append(yr + 'q' + qtr)
            # ---
        for period in periods: 
            print('___ ingesting ___' + period)
            self.ingest(period)





# __ make es document for each 





sub first
then num --> adsh

then tag --> tag/version 

then presentation --> tag, version, adsh









# --
# - instantiate classes & call functions


if args.class_opp not in ('RSS', 'AQFS'):
    print('must choose one: RSS, AQFS')
elif args.class_opp == 'RSS':
    k = RSS()
    k.SECdownload()
    k.unzip()
    # --- parse docs
    m = RSS_parse()
    m.parse_r()
    m.ingest()
elif args.class_opp == 'AQFS':
    a = AQFS()
    a.run()


