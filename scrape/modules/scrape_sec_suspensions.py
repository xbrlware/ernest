#!/usr/bin/env python2.7

'''
    Scrape and ingest new SEC trading suspensions from the EDGAR directory

    ** Note **
    This runs each day as suspensions are published on the rss feed
'''

import re
import time
import json
import logging
import pdfquery
from hashlib import md5

from bs4 import BeautifulSoup
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from urllib import urlopen

from generic.date_handler import DATE_HANDLER


class SEC_SCRAPER:
    def __init__(self, args):
        self.logger = logging.getLogger('scrape_halt.sec_scraper')
        self.args = args
        self.dh = DATE_HANDLER('scrape_halt.sec_scraper')
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config
        self.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        self.end_year = args.end_year
        self.current_year = datetime.now().year
        self.aka = re.compile('(\(*./k/a\)*) ([A-Za-z\.\, ]+)')
        self.btype_rex = re.compile(
            "(.*)(Inc|INC|Llc|LLC|Comp|COMP|Company|Ltd|LTD|Limited|Corp|CORP|\
            Corporation|CORPORATION|Co|N\.V\.|Bancorp|et al|Group)(\.*)")
        self.date_rex = re.compile(
            '[JFMASOND][aepuco][nbrynlgptvc]\.{0,1} \d{0,1}\d\, 20[0-1][0-7]')
        self.client = Elasticsearch([{
            "host": config['es']['host'],
            "port": config['es']['port']}])
        self.es_index = config['suspension']['index']
        self.doc_type = config['suspension']['_type']
        self.main_sleep = 0.5
        self.scrape_sleep = 0.5
        self.domain = "http://www.sec.gov"
        self.current_page_link = self.domain + "/litigation/suspensions.shtml"
        self.url_fmt = self.domain + \
            "/litigation/suspensions/suspensionsarchive/susparch{}.shtml"
        self.pdf_tmp_path = '/tmp/sec_temp_file.pdf'
        self.xml_tmp_path = '/tmp/sec_temp_file.xml'

    def link2release_number(self, link):
        return '-'.join(link.split('/')[-1:][0].split('-')[:-1])

    def enrich_dates(self):
        query = {
            "query": {
                "filtered": {
                    "filter": {
                        "missing": {
                            "field": "_enrich.halt_short_date"
                        }
                    }
                }
            }
        }
        for doc in scan(self.client,
                        index=self.config['otc_halts']['index'],
                        doc_type=self.config['otc_halts']['_type'],
                        query=query):
            body = doc['_source']

            # not sure if necessary... might be differances between old and new
            try:
                dh = body['DateHalted']
            except:
                dh = body['doc']['DateHalted']

            try:
                ld = body['LoadDate']
            except:
                ld = body['doc']['LoadDate']

            body['_enrich'] = {}
            body['_enrich']['halt_short_date'] = self.dh.ref_date(dh)
            body['_enrich']['halt_long_date'] = self.dh.long_date(dh)
            body['_enrich']['load_short_date'] = self.dh.ref_date(ld)
            body['_enrich']['load_long_date'] = self.dh.long_date(ld)

            self.client.index(
                index=self.config['otc_halts']['index'],
                doc_type=self.config['otc_halts']['_type'],
                id=doc["_id"],
                body=body)

    def grab_dates(self, soup_object):
        dates = []
        for ele in soup_object.findAll('td'):
            if re.match(self.date_rex, ele.text):
                d = re.match(self.date_rex, ele.text).group(0)
                d = re.sub(',|\.', '', d)
                d = datetime.strptime(d, "%b %d %Y").strftime('%Y-%m-%d')
                dates.append(d)
        return dates

    def grab_links(self, soup_obj):
        atags = soup_obj('a', href=True)
        links = [a['href'] for a in atags if '-o.pdf' in a['href']]
        return [self.domain + link for link in links]

    def pdf_link2soup(self, link):
        xml_path = '%s-%s' % (self.xml_tmp_path, md5(link).hexdigest())

        # Link -> PDF
        pdf_content = urlopen(link).read()
        open(self.pdf_tmp_path, 'wb').write(pdf_content)

        # PDF -> XML
        pdf = pdfquery.PDFQuery(self.pdf_tmp_path,
                                merge_tags=('LTChar'),
                                round_floats=True,
                                round_digits=3,
                                input_text_formatter=None,
                                normalize_spaces=False,
                                resort=True,
                                parse_tree_cacher=None,
                                laparams={'all_texts': True,
                                          'detect_vertical': False})

        pdf.load()
        pdf.tree.write(xml_path)

        # XML -> Soup
        xml_content = open(xml_path, 'r').read()
        return BeautifulSoup(xml_content, 'xml')

    def pdf_link2companies(self, link):
        companies = []
        state = {"str": "", "flag": False}

        for ele in self.pdf_link2soup(link).findAll('LTTextLineHorizontal'):
            text = ele.text.strip()
            search_btype = re.search(self.btype_rex, text)
            search_aka = re.search(self.aka, text)

            if search_btype:
                if search_aka:
                    orig_val = search_aka.group(2).strip()
                else:
                    orig_val = '%s %s' % (
                        search_btype.group(1).strip(),
                        search_btype.group(2).strip())

                if state['flag']:
                    val = orig_val
                else:
                    self.logger.info('flag %s' % state['str'])
                    val = ('%s %s' % (state['str'], orig_val)).strip()

                companies.append(val)
                state['str'] = orig_val
            else:
                if len(companies) > 0:
                    break

                state['str'] = text if len(text) > 1 else ''

            state['flag'] = True if search_btype else False

        return companies

    def scrape_year(self, page_link):
        ''' Get all companies suspended for a given year'''
        self.logger.info("Downloading Index %s" % page_link)

        soup = BeautifulSoup(urlopen(page_link).read(), 'xml')

        objs = zip(self.grab_dates(soup), self.grab_links(soup))
        objs = [{'date': x[0],
                 'link': x[1],
                 'release_number': self.link2release_number(x[1])
                 } for x in objs]
        objs = filter(lambda x: x['date'] > self.start_date.strftime(
            '%Y-%m-%d'), objs)

        for x in objs:
            self.logger.info("Downloading PDF %s" % x['link'])

            for company in self.pdf_link2companies(x['link']):
                body = {
                    "release_number": x['release_number'],
                    "link": x['link'],
                    "date": x['date'],
                    "company": company
                }

                _id = (body['date'] +
                       '__' + body['release_number'] +
                       '__' +
                       body['company'].replace(' ', '_')).replace('-', '')
                try:
                    self.client.create(
                        index=self.es_index,
                        doc_type=self.doc_type, id=_id, body=body)
                except:
                    self.logger.info('document already exists')

            time.sleep(self.scrape_sleep)

    def main(self):
        ''' Iterate over years, downloading suspensions '''
        resp = self.client.count(index=self.es_index)
        count_in = resp['count'] or None

        if self.args.most_recent:
            self.scrape_year(self.current_page_link)
        elif not self.args.most_recent:
            years = range(self.start_date.year, self.end_year + 1)[::-1]
            for year in years:
                if year == self.current_year:
                    # Current year has different link format
                    self.scrape_year(self.current_page_link)
                else:
                    page_link = self.url_fmt.format(year)
                    self.scrape_year(page_link)

                time.sleep(self.main_sleep)

        resp = self.client.count(index=self.es_index)
        count_out = resp['count'] or None
        self.logger.info('{0} in, {1} out'.format(count_in, count_out))
        self.logger.info('enriching halt dates')
        self.enrich_dates()
        self.logger.info('enriching halt dates finished')
        return [count_in, count_out]
