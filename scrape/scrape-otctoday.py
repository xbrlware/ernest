#!/usr/bin/env python2

from bs4 import BeautifulSoup

import datetime
import json
import urllib2
import re
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# --
# CLI

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
parser.add_argument("--update", action='store_true')
parser.add_argument("--getall", action='store_true')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)

# - 
# global variables

now = datetime.datetime.now()

dateDict = { 
    "Jan" : 1, 
    "Feb" : 2, 
    "Mar" : 3, 
    "Apr" : 4, 
    "May" : 5, 
    "Jun" : 6, 
    "Jul" : 7, 
    "Aug" : 8, 
    "Sep" : 9, 
    "Oct" : 10, 
    "Nov" : 11, 
    "Dec" : 12
}

# - 
# functions

def num_days(dateJS, tDate):
    x = dateJS.split('/')
    y = [x[0][-2:], x[1][-2:], x[2][:4]]
    z = datetime.date(int(y[2]), int(y[0]), int(y[1]))
    zz = tDate - z
    return zz.days + 1


def toDate(date, now): 
    m   = dateDict[re.findall('\D{3}', date)[0]]
    date = str(now.year) + '-' + str(m).zfill(2) + '-' + str(now.day)
    return date



# - 
# build out

final_array = []
today = datetime.date.today()

html = urllib2.urlopen('http://www.theotc.today/').read()

bs = BeautifulSoup(html, 'lxml')

table = bs.find('table', {'class': 'alternaterow'})
rows = table.findAll('tr')

for ele in rows[2:]:
    a = []
    ourDict = {}
    row = ele.findAll('td')
    for r in row:
        span = r.findAll('span')
        try:
            a.append(span[0].text)
            if len(span) > 1:
                a.append(int(span[1].text))
        except:
            a.append(r.text)
    if len(a) > 7:
        ourDict['date'] = a[0]
        ourDict['ticker'] = a[1]
        ourDict['times_promoted'] = a[2]
        ourDict['previous_ticker'] = a[3]
        ourDict['days'] = num_days(a[4], today)
        ourDict['promoters'] = a[5].split()
        ourDict['shares'] = a[6]
        ourDict['value'] = a[7]
        ourDict['launch_price'] = a[8]
        final_array.append(ourDict)


# - 
# Run 

if args.update: 
    for i in final_array: 
        doc = final_array[i]
        d = int(re.findall('\d{1,}', doc['date'])[0]) 
        if d >= now.day -1: 
            doc['date'] = toDate(doc['date'], now)
            client.index(
                index    = 'ernest_otctoday_touts', 
                doc_type = 'tout', 
                id       = doc['date'] + '_' + doc['ticker'], 
                body     = doc
            )
        else: 
            pass
elif args.getall: 
    for i in final_array: 
        doc = final_array[i]
        doc['date'] = toDate(doc['date'], now)
        client.index(
            index    = 'ernest_otctoday_touts', 
            doc_type = 'tout', 
            id       = doc['date'] + '_' + doc['ticker'], 
            body     = doc
        )

