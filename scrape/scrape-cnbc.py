#!/usr/bin/env python
"""
1.) not all tickers are showing up
2.) 'body' is showing up in the index

"""
import hashlib
import re
import requests
import time

from bs4 import BeautifulSoup
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from itertools import chain


re_ticker = re.compile('[A-Z]+[A-Z0-9]+ *: *[A-Z]+[A-Z0-9]+')
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
    return {'market': s[0], 'symbol': s[1]}


def find_tickers(line):
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


def parse_page(page):
    i = []
    ii = {}
    p = [re_line(p) for p in page.find('div', id='article_body').div.div]
    i = list(chain(*[find_tickers(l) for l in p if l is not None]))
    for ele in i:
        ii[ele['market']] = ele['symbol']

    t = page.find('time')['datetime']
    tt = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S%z')
    ttt = datetime.strftime(tt, '%Y-%m-%d')
    h = page.find('h1', class_='title').text
    return {
        "_op_type": "index",
        "_id": create_id(h),
        "_index": "ernest_cnbc",
        "_type": "article",
        "doc": {
            "headline": h,
            "date": ttt,
            "tickers": [{"exchange": ele, "symbol": ii[ele]} for ele in ii],
            "article": ''.join([x for x in p if x is not None])
            }
        }


def handle_link(link):
    p = parse_page(get_page(link))
    time.sleep(3)
    return p


def handle_links(links):
    return [handle_link(link) for link in links]


def main():
    b = 'http://www.cnbc.com/press-releases/'
    # c = '?page={}'
    for a, b in streaming_bulk(client,
                               actions=handle_links(
                                   get_links(b))):
        print(a, b)


if __name__ == '__main__':
    main()
