#!/usr/bin/env python

import hashlib
import re
import requests
import time

from bs4 import BeautifulSoup
from collections import OrderedDict
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk
from itertools import chain


re_ticker = re.compile('[A-Z]+[A-Za-z0-9]+ *: *[A-Z]+[A-Z0-9]+')
client = Elasticsearch(host="localhost", port=9205)


def get_page(url):
    return BeautifulSoup(requests.get(url).content, 'lxml')


def get_links(url):
    b = 'http://www.cnbc.com'
    p = get_page(url)
    divs = p.find_all('div', class_='headline')
    return [b + d.a['href'] for d in divs]


def split_ticker(ticker):
    s = ticker.split(':')
    if re.match(r'^[A-Z]{1,5}$', s[1]):
        return {'market': s[0], 'symbol': s[1]}
    else:
        return None


def find_tickers(line):
    if re.search('\(.*\)', line) is None:
        return []
    else:
        return [split_ticker(t) for t in re_ticker.findall(line)]


def re_line(line):
    try:
        p = line.text.strip()
        if len(p) > 0:
            return '<p>' + p + '</p>'
        else:
            return None
    except:
        return None


def create_id(headline):
    return hashlib.md5(headline.encode('utf-8')).hexdigest()


def parse_page(page, link):
    i = []
    ii = {}
    p = [re_line(p) for p in page.find('div', id='article_body').div.div]
    i = list(chain(*[find_tickers(l) for l in p if l is not None]))
    for ele in i:
        if ele is not None:
            ii[ele['market']] = ele['symbol']

    t = page.find('time')['datetime']
    tt = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S%z')
    ttt = datetime.strftime(tt, '%Y-%m-%d')
    h = page.find('h1', class_='title').text
    src = page.find('span', itemprop="sourceOrganization").text
    return {
        "_op_type": "index",
        "_id": create_id(h),
        "_index": "ernest_news",
        "_type": "article",
        "_source": {
            "cik": None,
            "sic": None,
            "doc": {
                "source": src,
                "scraper": "cnbc",
                "headline": h,
                "date": ttt,
                "url": link,
                "tickers":
                [{"exchange": ele, "symbol": ii[ele]} for ele in ii],
                "article": ''.join([x for x in p if x is not None])
                }
            }
        }


def fetch_cik_sic(p, ticker):
    q = {
        "query": {
            "match": {
                "ticker": ticker.lower()
                }
            }
        }
    cod = OrderedDict()
    for a in scan(client,
                  index='ernest_forms_raw',
                  doc_type='sub',
                  query=q):
        cod[a['_source']['period']] = {
            'cik': a['_source']['cik'],
            'sic': a['_source']['sic']
            }
    if len(cod) > 0:
        key = sorted(cod, reverse=True)[0]
        udoc = cod[key]
        p['_source']['cik'] = udoc['cik']
        p['_source']['sic'] = udoc['sic']
    return p


def handle_link(link):
    print(link)
    try:
        p = parse_page(get_page(link), link)
    except:
        p = None
    if p is not None and len(p['_source']['doc']['tickers']) > 0:
        p = fetch_cik_sic(
            p, p['_source']['doc']['tickers'][0]['symbol'])
    time.sleep(3)
    return p


def handle_links(links):
    x = [handle_link(link) for link in links]
    return [xx for xx in x if xx is not None]


def main():
    b_u = 'http://www.cnbc.com/press-releases/'
    c_u = '?page={}'
    for i in range(20, 30):
        for a, b in streaming_bulk(client,
                                   actions=handle_links(
                                       get_links(b_u + c_u.format(i))),
                                   raise_on_exception=False):
            print(i, a, b)


if __name__ == '__main__':
    main()
