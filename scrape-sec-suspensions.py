#!/usr/bin/env python

import re
import sys
import time
import json
import argparse
import pdfquery

from bs4 import BeautifulSoup
from datetime import datetime
from elasticsearch import Elasticsearch

if sys.version_info[0] < 3:
    from urllib import urlopen
else:
    from urllib.request import urlopen

AKA_REX   = re.compile('(\(*./k/a\)*) ([A-Za-z\.\, ]+)')
BTYPE_REX = re.compile('(.*)(Inc|INC|Llc|LLC|Comp|COMP|Company|Ltd|LTD|Limited|Corp|CORP|Corporation|CORPORATION|Co|N\.V\.|Bancorp|et al|Group)(\.*)')
DATE_REX  = re.compile('[JFMASOND][aepuco][nbrynlgptvc]\.{0,1} \d{0,1}\d, 20[0-1][0-6]')

class SECScraper:
    def __init__(self, config, start_date, end_year, stdout=False):
        self.stdout = stdout
        
        self.start_date   = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_year     = end_year
        self.current_year = datetime.now().year
        
        self.aka       = AKA_REX
        self.btype_rex = BTYPE_REX
        self.date_rex  = DATE_REX
        
        self.client   = Elasticsearch([config['es']['host']], port=config['es']['port'])
        self.es_index = config['suspension']['index']
        self.doc_type = config['suspension']['_type']
        
        self.main_sleep   = 0.5
        self.scrape_sleep = 0.5
        
        self.domain = "http://www.sec.gov"
        self.current_page_link = "http://www.sec.gov/litigation/suspensions.shtml"
        self.url_fmt = "http://www.sec.gov/litigation/suspensions/suspensionsarchive/susparch{}.shtml"

        self.pdf_tmp_path = '/tmp/sec_temp_file.pdf'
        self.xml_tmp_path = '/tmp/sec_temp_file.xml'
        
    def link2release_number(self, link):
        return '-'.join(link.split('/')[-1:][0].split('-')[:-1])
    
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
        pdf.tree.write(self.xml_tmp_path)
        
        # XML -> SOup
        xml_content = open(self.xml_tmp_path, 'r').read()
        return BeautifulSoup(xml_content, 'xml')

    def pdf_link2companies(self, link):
        companies = []
        state = {"str": "", "flag": False}
        
        soup = self.pdf_link2soup(link)
        for ele in soup.findAll('LTTextLineHorizontal'):
            m = re.search(self.btype_rex, ele.text)
            if m:
                n = re.search(self.aka, ele.text)
                if n:
                    orig_val = n.group(2).strip()
                else:
                    orig_val = '%s %s' % (m.group(1).strip(), m.group(2).strip())
                
                val = orig_val
                if not state['flag']:
                    val = '%s %s' % (state['str'], val)
                    
                companies.append(val.strip())
                state['str']  = orig_val
                state['flag'] = True
            else:
                if len(companies) > 0:
                    break
                
                state['str']  = ele.text.strip()
                state['flag'] = False

        return companies

    def scrape_year(self, page_link):
        ''' Get all companies suspended for a given year'''
        print >> sys.stderr, "Downloading Index \t %s" % page_link
        
        soup = BeautifulSoup(urlopen(page_link).read(), 'xml')
        
        objs = zip(self.grab_dates(soup), self.grab_links(soup))
        objs = [{'date' : x[0], 'link' : x[1], 'release_number' : self.link2release_number(x['link'])} for x in objs]
        objs = filter(lambda x: x['date'] > self.start_date.strftime('%Y-%m-%d'), objs)
        
        for x in objs:
            print >> sys.stderr, "Downloading PDF \t %s" % x['link']
            
            for company in self.pdf_link2companies(x['link']):
                body = {
                    "release_number" : x['release_number'], 
                    "link"           : x['link'], 
                    "date"           : x['date'], 
                    "company"        : company
                }
                
                if self.stdout:
                    print json.dumps(body)
                else:
                    self.client.index(index=self.es_index, doc_type=self.doc_type, body=body)
                           
            time.sleep(self.scrape_sleep)
    
    def main(self):
        ''' Iterate over years, downloading suspensions '''
        years = range(self.start_date.year, self.end_year + 1)[::-1]
        for year in years:
            if year == self.current_year:
                # Current year has different link format
                self.scrape_year(self.current_page_link)
            else:
                page_link = self.url_fmt.format(year)
                self.scrape_year(page_link)
                
            time.sleep(self.main_sleep)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='scrape_trade_suspensions')
    parser.add_argument("--config-path", type=str, action='store')
    parser.add_argument("--start-date", type=str, action='store', default="1994-12-31")
    parser.add_argument("--end-year", type=int, action='store', default=datetime.now().year)
    parser.add_argument("--stdout", action='store_true')
    args = parser.parse_args()
    
    config = json.load(open(args.config_path))

    SECScraper(config, args.start_date, args.end_year, args.stdout).main()
