#!/usr/bin/env python2.7

import logging
import json
import os
import re
import time
import urllib2
import zipfile

from bs4 import BeautifulSoup
from datetime import date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk


class OTC_SCRAPE:
    def __init__(self, args):
        self.logger = logging.getLogger('scrape_otc.otc_scrape')
        self.args = args
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            "host": config['es']['host'],
            "port": config['es']['port']}])

        if args.from_scratch:
            self.start_year = 2010
        elif args.most_recent:
            self.start_year = int(date.today().year)

    def text_date(self, string):
        try:
            raw_date = re.findall('\d{1,}-\D{1,}-\d{4}', string)[0]
            raw_time = re.findall('\d{2}:\d{2}:\d{2}', string)[0]
            return time.strftime(
                "%Y-%m-%d %H:%M:%S",
                time.strptime(raw_date+raw_time, "%d-%b-%Y%H%M%S"))
        except:
            pass

    def zip_date(self, string):
        try:
            raw_date = re.findall('\d{1,}/\d{1,}/\d{4}', string)[0]
            date_str = time.strftime(
                "%y-%m-%d",
                time.strptime(raw_date, "%d/%m/%y"))
            try:
                raw_time = re.findall('\d{1,}:\d{2}:\d{2}', string)[0]
                return date_str + ' ' + raw_time
            except:
                return date
        except:
            return None

    def handle_url(self, url):
        return urllib2.urlopen(url)

    def parse_txt(self, text_str, base_file):
        keys = None
        actions = []
        response = self.handle_url(base_file)
        for line in response:
            split_line = line.split('|')
            if keys is None:
                keys = split_line
            else:
                d = dict(zip(keys, split_line))
                out = {
                    'raw_source': 'txt_archive',
                    'source_doc': text_str,
                    'enrichDate': self.text_date(d['Daily List Date']),
                    'IssuerSymbol': d['New Symbol'].upper(),
                    'CompanyName': d['New Company Name'].upper(),
                    'Type': d['Type'].upper()
                }

                actions.append({
                    "_op_type": "index",
                    "_index": self.config['otc_raw']['index'],
                    "_type": self.config['otc_raw']['_type'],
                    "doc": out,
                    "_id": out['source_doc'].decode('utf-8') +
                    '_' + out['IssuerSymbol']
                    .decode('utf-8') +
                    '_' + str(out['enrichDate'])
                    .decode('utf-8') +
                    '_' + out['CompanyName']
                    .decode('utf-8') +
                    '_' + out['Type'].decode('utf-8')
                })

        return actions

    def parse_zip(self, text_str, base_file):
        read = self.handle_url(base_file)
        otc_dir = '/home/ubuntu/data/otc_archives/'
        zip_url = otc_dir + text_str + '.zip'
        dly_dir = otc_dir + 'Daily_List_' + text_str + '/'

        with open(zip_url, 'w') as inf:
            inf.write(read)
            inf.close()

        with zipfile.ZipFile(zip_url, 'r') as z:
            z.extractall(otc_dir)
            actions = []
            for i in os.listdir(dly_dir):
                name = str(i)
                with open(dly_dir + str(i), 'r') as dlyf:
                    x = dlyf.readlines()

                raw_doc = [line.split('|') for line in x]
                body = [[raw_doc[i][k].replace('\r\n', '')
                         for k in range(len(raw_doc[i]))]
                        for i in range(len(raw_doc))]

                for m in range(1, len(body)):
                    keys = body[0]
                    vals = body[m]
                    d = dict(zip(keys, vals))
                    if name[:2] == 'BB':
                        out = {
                            'raw_source': 'zip_archive',
                            'source_doc': name,
                            'enrichDate': self.zip_date(
                                d['DailyListDate']),
                            'IssuerSymbol': d['NewSymbol'].upper(),
                            'CompanyName': d['NewName'].upper(),
                            'Type': d['Type'].upper()
                        }
                    elif name[:2] == 'di':
                        out = {
                            'raw_source': 'zip_archive',
                            'source_doc': name,
                            'enrichDate': self.zip_date(
                                d['Daily List Date']),
                            'IssuerSymbol': d['Issue Symbol'].upper(),
                            'CompanyName': d['Company Name'].upper(),
                            'Type': 'DIVIDEND'
                        }

                    actions.append({
                        "_op_type": "index",
                        "_index": self.config['otc_raw']['index'],
                        "_type": self.config['otc_raw']['_type'],
                        "doc": out,
                        "_id": out['source_doc'].decode('utf-8') +
                        '_' + out['IssuerSymbol']
                        .decode('utf-8') +
                        '_' + str(out['enrichDate'])
                        .decode('utf-8') +
                        '_' + out['CompanyName']
                        .decode('utf-8') +
                        '_' + out['Type'].decode('utf-8')
                    })

        return actions

    def main(self):
        soup = BeautifulSoup(
            self.handle_url(
                'http://otce.finra.org/DailyList/Archives'))
        links = soup.find("ul", {"class": ['rawlist']}).findAll("li")

        for link in links:
            x = link.find('a')
            _file = x['href']
            _text = x.get_text()
            base = 'http://otce.finra.org'
            base_file = base + _file

            if 'txt' in str(_text):
                year = int(re.sub('\D', '', _text)[4:])
                if year >= self.start_year:
                    for a, b in parallel_bulk(self.client,
                                              self.parse_txt(_text, base_file)):
                        if a is not True:
                            self.logger.info(a, b)
                else:
                    pass
            elif 'zip' in str(_file):
                year = int(_text)
                if year >= self.start_year:
                    for a, b in parallel_bulk(self.client,
                                              self.parse_zip(_text, base_file)):
                        if a is not True:
                            self.logger.info(a, b)
            else:
                pass
