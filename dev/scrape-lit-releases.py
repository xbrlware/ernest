
##############################Scrape & Index Sec Litigation Publications###############


import bs4
from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import csv
import urllib2
import os
## Dont need these, but as you move forward with this script you will run into error handling
from urllib2 import Request, urlopen
from urllib2 import URLError
from urllib2 import HTTPError
from StringIO import StringIO
import datetime
from datetime import date, timedelta



##################### I: Define function library


def get_links(rows): 
    links = []
    x = re.compile('litreleases\W\d{4}\Wlr\d{1,}\Whtm')
    for i in rows: 
        try: 
            link = i['href']
            if re.findall(x, link): 
                links.append(link)
        except KeyError, e: 
            print(e)
    return links

#### Working to get more historical data through versions

def get_data(data): 
    sec_lit = []
    for link in data[0]: 
        url = root + str(link)
        print(url)
        try: 
            obj  = BeautifulSoup(urllib2.urlopen(url))
            title           = obj.find('head').find('meta', {'name' : ['title']}) if (obj.find('head').find('meta', {'name' : ['title']}) is None) else obj.find('head').find('meta', {'name' : ['title']})['content']
            date            = obj.find('head').find('meta', {'name' : ['date']}) if (obj.find('head').find('meta', {'name' : ['date']}) is None) else obj.find('head').find('meta', {'name' : ['date']})['content']
            lit_identifier  = obj.find('head').find('meta', {'name' : ['identifier']}) if (obj.find('head').find('meta', {'name' : ['identifier']}) is None) else obj.find('head').find('meta', {'name' : ['identifier']})['content']
            desc_hdr        = obj.find('h3') if (obj.find('h3') is None) else obj.find('h3').get_text() 
            text            = obj.findAll('p')[0].get_text() if isinstance(obj.findAll('p')[0], object) else None
            text2           = obj.findAll('p')[1].get_text() if isinstance(obj.findAll('p')[1], object) else None
            a = [title, date, lit_identifier, desc_hdr, text, text2]
            sec_lit.append(a)
        except URLError, e: 
            print(url)
            print(e)
    ##df = pd.DataFrame(sec_lit)
    return sec_lit


## Need to get 'h2' tags
## Need to get head.desription tags




# def get_data(data): 
#     sec_lit = []
#     for link in data[0]: 
#         url = root + str(link)
#         print(url)
#         try: 
#             obj  = BeautifulSoup(urllib2.urlopen(url))
#             title           = obj.find('head').find('meta', {'name' : ['title']}) if (obj.find('head').find('meta', {'name' : ['title']}) is None) else obj.find('head').find('meta', {'name' : ['title']})['content']
#             date            = obj.find('head').find('meta', {'name' : ['date']}) if (obj.find('head').find('meta', {'name' : ['date']}) is None) else obj.find('head').find('meta', {'name' : ['date']})['content']
#             lit_identifier  = obj.find('head').find('meta', {'name' : ['identifier']}) if (obj.find('head').find('meta', {'name' : ['identifier']}) is None) else obj.find('head').find('meta', {'name' : ['identifier']})['content']
#             desc_hdr        = obj.find('h3') if (obj.find('h3') is None) else obj.find('h3').get_text() 
#             text            = obj.findAll('p')[0].get_text() if isinstance(obj.findAll('p')[0], object) else None
#             text2           = obj.findAll('p')[1].get_text() if isinstance(obj.findAll('p')[1], object) else None
#             a = [title, date, lit_identifier, desc_hdr, text, text2]
#             sec_lit.append(a)
#         except URLError, e: 
#             print(url)
#             print(e)
#     ##df = pd.DataFrame(sec_lit)
#     return sec_lit




def coerce_out(x): 
    tmp = {
        "title"         : (x['title']), 
        ##"date"          : str(x['date']),
        "case_no"       : (x['case_no']),
        "header"        : (x['header']),
        "par1"          : (x['par1']),
        "par2"          : (x['par2'])
    }
    return ('-', tmp)



def get_dirs(): 
    urls = []
    for i in range(2010, date.today().year + 1): 
        if i == date.today().year: 
            url = 'http://www.sec.gov/litigation/litreleases.shtml'
        else: 
            main = 'http://www.sec.gov/litigation/litreleases/litrelarchive/litarchive'
            url = main + str(i) + '.shtml'
        urls.append(url)
    return urls



##################### II: Scrape releases and build RDD

## Get all repositories 2010-present & extract links
urls = []
urls.append(get_dirs())
urls = urls[0]

data = pd.DataFrame()
for url in urls: 
    html = urllib2.urlopen(url) 
    obj  = BeautifulSoup(html)
    rows = obj.findAll('a')
    data = data.append(get_links(rows))



## Scrape links, structure data and build RDD
root = 'http://www.sec.gov'
lit_releases = pd.DataFrame()
t_releases = lit_releases.append(get_data(data))
lit_releases.columns = ('title', 'date', 'case_no', 'header', 'par1', 'par2')
y = lit_releases.transpose().to_dict().values()
sec_lit = sc.parallelize(y)
sec_lit_out = sec_lit.map(coerce_out)



config = {
    'hostname' : 'localhost',
    'hostport' : 9200
 }

write_index_name = 'litigation'
write_index_type = 'sec_text'
##path_to_id       = 'case_no'  

sec_lit_out.saveAsNewAPIHadoopFile(
    path              = '-',
    outputFormatClass = "org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass          = "org.apache.hadoop.io.NullWritable", 
    valueClass        = "org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf              = {
        "es.nodes"           : config['hostname'], # eg localhost
        "es.port"            : str(config['hostport']), # eg 9200
        "es.resource"        : write_index_name + "/" + write_index_type,
        ##"es.mapping.id"      : path_to_id, # optional
        "es.write.operation" : "index" # create if id doesn't exist, replace if it does
    }
)












