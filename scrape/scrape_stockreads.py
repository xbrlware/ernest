#!/usr/bin/env python

"""
    Scrape and ingest new / historical stock touts from stockreads

    ** Note **
    This runs prospectively using the --most-recent argument
"""

import json
import re
import argparse
import urllib2
import datetime
from datetime import timedelta
from collections import Counter

from bs4 import BeautifulSoup

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


parser = argparse.ArgumentParser(description='Scrape stockreads touts')
parser.add_argument('--from-scratch', dest='from_scratch', action="store_true")
parser.add_argument('--min-page', type=int, dest='min_page',
                    action="store", default=81000)
parser.add_argument('--most-recent', dest='most_recent', action="store_true")
parser.add_argument('--config-path', type=str,
                    action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open('/home/ubuntu/ernest/config.json'))

HOSTNAME = config['es']['host']
HOSTPORT = config['es']['port']

INDEX = config['touts']['index']
TYPE = config['touts']['_type']

client = Elasticsearch([{'host': HOSTNAME, 'port': HOSTPORT}])


def get_max_page():
    global config
    query = {
        "size": 0,
        "aggs": {"max": {"max": {"field": "page_no"}}}
    }
    d = client.search(index=INDEX, body=query)
    return int(d['aggregations']['max']['value'])


def get_newest_id():
    url = 'http://stockreads.com/'
    soup = BeautifulSoup(urllib2.urlopen(url), 'lxml')
    link = soup.find('div', {'class': ['recentTitle']}).findAll('a')[0]['href']
    newestID = int(re.sub('[^0-9]', '', link))
    return newestID


def date_edges(tbl):
    if 'Yesterday' in tbl:
        date = str(datetime.date.today() + timedelta(days=-1))
        date = tbl.replace('Yesterday', date)
    elif 'Today' in tbl:
        date = str(datetime.date.today())
        date = tbl.replace('Today', date)
    else:
        t = tbl.replace('/', 'oo')
        c = re.compile('\d{1,}oo\d{1,}oo\d{4}')
        parts = re.findall(c, t)[0].split('oo')
        d = parts[2] + '-' + parts[0].zfill(2) + '-' + parts[1]
        date = re.sub('\d{1,}oo\d{1,}oo\d{4}', d, t)
    return date


def get_title(soup):
    try:
        title = soup.find(
            'div', {'id': ['divStockNewsletter']}).find('h1').getText()
    except:
        title = None
    return title


def get_author(soup):
    try:
        tbl = soup.find(
            'div',
            {'id': ['divStockNewsletter']}).findAll(
                'div', {'style': ['float:left']})
        tag = BeautifulSoup(str(tbl), 'lxml').find('a')['href']
        pat = re.compile('name=.*&from=')
        author = re.findall(pat, tag)[0].replace('name=',
                                                 '').replace('&from=',
                                                             '').replace('+',
                                                                         ' ')
    except:
        author = None
    return author


def get_date(soup):
    try:
        sec = soup.find(
            'div', {'id': ['divStockNewsletter']}).findAll(
                'div', {'style': [
                    'float:left;margin-left:5px;margin-right:20px;'
                    ]})[0].getText()
        date = date_edges(sec)
        date = datetime.datetime.strptime(
            date, '%Y-%m-%d %I:%M %p').strftime('%Y-%m-%d %H:%M:%S')
    except:
        date = None
    return date


def get_mentions(soup):
    try:
        m = [i.getText() for i in soup.findAll('a') if 'By-Symbol' in str(i)]
    except:
        m = None
    return m


def get_content(soup):
    try:
        content = soup.find('div', {'id': ['divStockNewsletter']}).getText()
    except:
        content = None
    return content


def get_stockreads(page_no):
    url = 'http://stockreads.com/Stock-Newsletter.aspx?id=' + str(page_no)
    soup = BeautifulSoup(urllib2.urlopen(url), 'lxml')
    title = get_title(soup)
    author = get_author(soup)
    date = get_date(soup)
    mentions = get_mentions(soup)
    content = get_content(soup)
    out = {
        "title": title,
        "author": author,
        "date": date,
        "content": content,
        "mentions": mentions,
        "page_no": page_no
    }
    if len(out['content']) < 1000:
        out = {key: None for (key, value) in out.iteritems()}
    return out


def clean_stockreads(out):
    if out['mentions'] is not None:
        x = Counter(out['mentions'])
        x = [{
            'ticker': key.encode('ascii', 'ignore').replace('.', '&'),
            'freq': value} for (key, value) in x.iteritems()]
        out['mentions'] = x
    elif out['mentions'] is None:
        out['mentions'] = []
    yield {
        "_id": out['page_no'],
        "_type": TYPE,
        "_index": INDEX,
        "_op_type": "index",
        "_source": out
    }


def run_upload_touts(min_page, max_page):
    for i in range(min_page, max_page):
        for a, b in streaming_bulk(client,
                                   clean_stockreads(get_stockreads(i)),
                                   chunk_size=500):
            print(a)
            print(i)

if args.from_scratch:
    min_page = args.min_page
    runUploadTouts(min_page, getNewestID())
elif args.most_recent:
    min_page = getMaxPage()
    runUploadTouts(min_page, getNewestID())
