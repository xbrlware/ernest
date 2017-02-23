#!/usr/bin/env python

import re
import sys
import time
import urllib2

from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from datetime import datetime

client = Elasticsearch(host='localhost', port='9205')

class OMX_SCRAPE_HISTORICAL:
    def __init__(self):
        self.base_url = 'http://inpublic.globenewswire.com'
        self.headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/55.0.2883.87 Chrome/55.0.2883.87 Safari/537.36'}
        self.re_tickers = re.compile('([A-Z]+ *: *[A-Z]+\.*[A-Z]*)')
        self.re_permalink = re.compile('.*:permalink') 

    def date_handle(self, date_string):
        return datetime.strptime(date_string, "%Y-%m-%d").strftime("%Y-%m-%d")

    def get_tickers(self, html):
        f_list = []
        ticker_list = list(set(self.re_tickers.findall(html)))
        try:
            for t in ticker_list:
                ts = t.split(':')
                f_list.append({'exchange': ts[0].strip(), 'symbol': ts[1].strip()})
        except:
            print >> sys.stderr, "No Tickers"

        return f_list

    def parse_article(self, soup):
        ps = soup.find_all('p', {'class': 'hugin'})
        return '\n'.join([ele.text for ele in ps])

    def parse_release(self, html, soup):
        pr = {
            "url": "{0}{1}".format(self.base_url, soup.find('a', {'id': self.re_permalink})['href'].split(';')[0]),
            "id": soup.find('a', {'id': self.re_permalink})['href'].split(';')[0].split('HUG')[1].split('.')[0],
            "tickers": self.get_tickers(html),
            "title": soup.find('meta', {'name': 'og:title'})['content'],
            "date":  self.date_handle(soup.find('span', {'class': 'date'}).text.strip()),
            "headline": soup.find('div', {'class': 'release-detail'}).find('h1').text,
            "article": self.parse_article(soup),
            "company": {"company": soup.find('div', {'class': 'company-name'}).find('a').text, "symbol" : None, "location" : None}
        }
        return client.index(index='ernest_omx_cat', doc_type='article', id=pr['id'], body=pr)

    def main(self, lower, upper):
        for rid in range(lower, (upper + 1)):
            url = 'http://inpublic.globenewswire.com/hol/releaseDetails.faces?rId=' + str(rid)
            req = urllib2.Request(url, headers=self.headers)
            try:
                html = urllib2.urlopen(req).read()
                soup = BeautifulSoup(html, 'lxml')
                print >> sys.stderr, self.parse_release(html, soup)
            except:
                print >> sys.stderr, "Could not get :: %s" % url
            time.sleep(2)

if __name__ == "__main__":
    omx = OMX_SCRAPE_HISTORICAL()
    omx.main(1939669, 2071916)
