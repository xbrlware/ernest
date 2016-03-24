



###### Extremely rough draft ETL process for xbrl documents

## loss of data at just about every stage in the process
## one-off methods for dealing with data quality issues (no systematic deduplication)
## nothing written in functional form; so readability is poor
## significant reduncancy in program structure; must be condensed

## last point especially true after item d in the third section of the process


######################## Clean ingestion proceedure for xbrl 10-K documents #######################

import bs4
from bs4 import BeautifulSoup
import feedparser
import os.path
import sys, getopt
import time
import socket
import urllib2 
from urllib2 import urlopen
from urllib2 import URLError
from urllib2 import HTTPError
import requests
from requests import TimeoutError
import xml.etree.ElementTree as ET
import zipfile
import zlib
import os
import csv
from collections import Counter
import re


-------Function Library I: pre parse downloading function (no errors in process)


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
        # except TimeoutError as e:
        #     print( "Timeout Error:", e.reason )
        except socket.timeout:
            print( "Socket Timeout Error" )
        except: 
            print('TimeoutError')
        if good_read:
            output = open( targetfname, 'wb' )
            output.write( mem_file )
            output.close()
        return good_read




def SECdownload(year, month):
    root = None
    feedFile = None
    feedData = None
    good_read = False
    itemIndex = 0
    edgarFilingsFeed = 'http://www.sec.gov/Archives/edgar/monthly/xbrlrss-' + str(year) + '-' + str(month).zfill(2) + '.xml'
    print( edgarFilingsFeed )
    if not os.path.exists( "sec/" + str(year) ):
        os.makedirs( "sec/" + str(year) )
    if not os.path.exists( "sec/" + str(year) + '/' + str(month).zfill(2) ):
        os.makedirs( "sec/" + str(year) + '/' + str(month).zfill(2) )
    target_dir = "sec/" + str(year) + '/' + str(month).zfill(2) + '/'
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
        except KeyError as e:
            print( "Key Error:", e )
        finally:
            print( "----------" )
        itemIndex += 1




-------Function Library II: post downloading parse (R)


library(Rcpp)
library(rvest)
library(XML)
library(XBRL)
options(stringsAsFactors = TRUE)

newdir = '/Users/culhane/Desktop/xbrl_downloads/2013/02'
unzippedFiles<-list.files(newdir)
finalDir<-file.path(newdir,'parsed')
dir.create(finalDir, showWarnings = FALSE) 


for(u in unzippedFiles){
    
    if(length(grep(pattern="[[:digit:]].xml", x=u))==1) { 
        print(u) 
        Test <- file.path(newdir,u) 
        docTest <- xbrlParse(Test)                     
        fctTest <- xbrlProcessFacts(docTest)
        title<-gsub("-|.xml", "", u)  
        write.table(fctTest, file = file.path(finalDir,paste0(title,'.csv')), sep = "," , append = TRUE)    
        
    }
}

### significant bottleneck in process here; drops a ton of documents for error (exception)
### 'Extra content at the and of the document'



-------Function Library III: post parse csv clean


###### part I: 

## all of the sudden stopped working for no reason

path14 = '/Users/culhane/Desktop/xbrl_downloads/2014/02/parsed'
year   = '2013'

path15 = '/Users/culhane/Desktop/xbrl_downloads/2015/02/parsed'
year   = '2014'

path13 = '/Users/culhane/Desktop/xbrl_downloads/2013/02/parsed'
year   = '2012'


filings = []
errors  = []


for x in os.listdir(path13):
    doc = path13 + '/' + x
    f = open(doc, 'rb') 
    reader = csv.reader(f)
    try: 
        your_list = list(reader)
        clean_doc = []
        for i in your_list: 
            if len(i) != 10: 
                print ('malformed')
            elif len(i) == 10: 
                try: 
                    int(i[0])
                    q    = re.compile('dei_')
                    z    = re.compile('us-gaap_')
                    gaap = re.findall(z, i[1])
                    dei  = re.findall(q, i[1])
                    if gaap: 
                        i.append(x)
                        i.append(year)
                        clean_doc.append(i)
                    elif dei: 
                        i.append(x)
                        i.append(year)
                        clean_doc.append(i)
                    else: 
                        print('custom tag')
                except ValueError: 
                    print ('malformed')
            else: 
                print('unknown error')
        ##print(clean_doc[1])
        filings.append(clean_doc)
        f.close()
    except:
        errors.append([x, 'error_null'])
        ##print('error_null')
        f.close()


###### part II: here the goal is to integrate this process into a more terse expression



### a) Get only relevant tags; log documents by central index; get only 10-K documents

dei  = ('dei_DocumentFiscalPeriodFocus', 'dei_DocumentFiscalYearFocus', 'dei_DocumentType', 'dei_DocumentPeriodEndDate', 'dei_EntityCentralIndexKey', 'dei_EntityCommonStockSharesOutstanding')
gaap = ('us-gaap_Assets', 'us-gaap_Liabilities', 'us-gaap_NetIncomeLoss', 'us-gaap_StockholdersEquity', 'us-gaap_Revenues', 'us-gaap_RetainedEarningsAccumulatedDeficit', 'us-gaap_OperatingIncomeLoss', 'us-gaap_InterestExpense', 'us-gaap_LiabilitiesAndStockholdersEquity', 'us-gaap_AssetsCurrent', 'us-gaap_LiabilitiesCurrent', 'us-gaap_CashAndCashEquivalentsAtCarryingValue')

## add company name
dei  = ('dei_DocumentFiscalPeriodFocus', 'dei_DocumentFiscalYearFocus', 'dei_DocumentType', 'dei_DocumentPeriodEndDate', 'dei_EntityCentralIndexKey', 'dei_EntityCommonStockSharesOutstanding')
gaap = ('us-gaap_Assets', 'us-gaap_Liabilities', 'us-gaap_RetainedEarningsAccumulatedDeficit', 'us-gaap_LiabilitiesAndStockholdersEquity', 'us-gaap_AssetsCurrent', 'us-gaap_LiabilitiesCurrent', 'us-gaap_CashAndCashEquivalentsAtCarryingValue')


subset  = []
ciks    = []
for i in filings: 
    tags   = [] 
    submis = []
    for x in i: 
        if (x[11] in x[2] and not '_dei_' in x[2]): ## only deduplication criteria 
            if x[1] in (dei): ## set of dei tags
                if x[1] == 'dei_EntityCentralIndexKey': 
                    submis.append(x[4])
                    submis.append(x[11])
                    tags.append(x)
                else: 
                    tags.append(x)
            elif x[1] in (gaap): ## set of gaap tags 
                tags.append(x) 
            else: 
                continue
        else: 
            continue
### check for 10-K documents only
    if len(tags) > 0: 
        check = []
        for a in tags: 
            if a[1] == 'dei_DocumentType' and a[4] in ('10-K', '10K'):
                check.append(tags)
            else: 
                continue
        if len(check) > 0: 
            subset.append(tags)
        else: 
            continue
    else: 
        continue
### make sure document has relevant content
    if len(submis) > 0: 
        ciks.append(submis)
    else: 
        continue


### b) This could be integrated above or really just should be combined into a single expres


complete = []
ids = []
for i in ciks: 
    if len(i) == 2: 
        ids.append(i[0])
    else: 
        continue

entries = dict(Counter(ids))
for key, value in entries.iteritems():
    if value == 3: 
        complete.append(key)
        complete.append(value)   




#### c) structuring data (ensure 3 filings per cik)

ad = []

for i in subset: 
    for x in i: 
        if x[1] == 'dei_EntityCentralIndexKey': 
            if x[4] in complete: 
                ad.append(i)
            else: 
                continue
        else: 
            continue



#### d) this is a horrible function (basically just saying does it have all the tags and pnly one of each)

sub = []
for i in ad: 
    tags1   = [] 
    tags2   = []
    tags3   = []
    tags4   = []
    tags5   = []
    tags6   = []
    tags7   = []
    for x in i: 
        if x[1] == 'us-gaap_CashAndCashEquivalentsAtCarryingValue': 
            tags1.append(x)
        elif x[1] == 'us-gaap_Liabilities': 
            tags2.append(x)
        elif x[1] == 'us-gaap_Assets': 
            tags3.append(x)
        elif x[1] == 'us-gaap_RetainedEarningsAccumulatedDeficit': 
            tags4.append(x)
        elif x[1] == 'us-gaap_LiabilitiesAndStockholdersEquity': 
            tags5.append(x)
        elif x[1] == 'us-gaap_AssetsCurrent': 
            tags6.append(x)
        elif x[1] == 'us-gaap_LiabilitiesCurrent': 
            tags7.append(x)
        else: 
            continue
    if len(tags1) == 1 and len(tags2) == 1 and len(tags3) == 1 and len(tags4) == 1 and len(tags5) == 1 and len(tags6) == 1 and len(tags7) ==1: 
        sub.append(i)
    else: 
        continue



#### e) define new narrow range of tags based on availability and move on

dei  = ('dei_DocumentFiscalPeriodFocus', 'dei_DocumentFiscalYearFocus', 'dei_DocumentType', 'dei_DocumentPeriodEndDate', 'dei_EntityCentralIndexKey', 'dei_EntityCommonStockSharesOutstanding')
gaap = ('us-gaap_Assets', 'us-gaap_Liabilities', 'us-gaap_RetainedEarningsAccumulatedDeficit', 'us-gaap_LiabilitiesAndStockholdersEquity', 'us-gaap_AssetsCurrent', 'us-gaap_LiabilitiesCurrent', 'us-gaap_CashAndCashEquivalentsAtCarryingValue')


analysis = []
for i in sub: 
    tags   = [] 
    for x in i: 
        if x[1] in (dei): ## set of dei tags
            tags.append(x)
        elif x[1] in (gaap): ## set of gaap tags 
            tags.append(x) 
        else: 
            continue
    if len(tags) > 0: 
        analysis.append(tags)
    else: 
        continue


records = []
for i in analysis: 
    record = {}
    for x in i: 
        if x[1] == 'dei_EntityCentralIndexKey':
            r = {'cik' : str(x[4])}
            record.update(r)
        elif x[1] == 'dei_DocumentPeriodEndDate':
            r = {'end_date' : x[4]}
            record.update(r)
        elif x[1] == 'us-gaap_Assets':
            r = {'Assets' : x[4]} 
            record.update(r)
        elif x[1] == 'us-gaap_AssetsCurrent':
            r = {'AssetsCurrent' : x[4]} 
            record.update(r)
        elif x[1] == 'us-gaap_Liabilities':
            r = {'Liabilities' : x[4]} 
            record.update(r)
        elif x[1] == 'us-gaap_LiabilitiesCurrent':
            r = {'LiabilitiesCurrent' : x[4]} 
            record.update(r)
        elif x[1] == 'us-gaap_RetainedEarningsAccumulatedDeficit':
            r = {'Earnings' : x[4]} 
            record.update(r)
        elif x[1] == 'us-gaap_CashAndCashEquivalentsAtCarryingValue':
            r = {'Cash' : x[4]} 
            record.update(r)
        elif x[1] == 'us-gaap_LiabilitiesAndStockholdersEquity':
            r = {'LiabilitiesEquity' : x[4]} 
            record.update(r)
        else: 
            continue
                    
    records.append(record)





keys = records[0].keys()
with open('/Users/culhane/Desktop/analysis_data_xbrl_updated2.csv', 'wb') as output_file:
    dict_writer = csv.DictWriter(output_file, keys)
    dict_writer.writeheader()
    dict_writer.writerows(records)


output_file.close()









### getting the filer name into the document

'dei_EntityRegistrantName'

















