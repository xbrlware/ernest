#!/usr/bin/env python

import pdfquery
import re
import sys
import time

from bs4 import BeautifulSoup
from datetime import datetime
from elasticsearch import Elasticsearch

if sys.version_info[0] < 3:
    from urllib import urlopen
else:
    from urllib.request import urlopen


class ScrapeSEC:
    def __init__(self):
        self.aka = re.compile('(\(*./k/a\)*) ([A-Za-z\.\, ]+)')
        self.btypes = re.compile(
            '(.*)(Inc|INC|Llc|LLC|Comp|COMP|Ltd|LTD|Corp|CORP)(\.*)')
        self.date_rex = re.compile(
            '[JFMASOND][aepuco][nbrynlgptvc]\.{0,1} [0-3][0-9], 20[0-1][0-6]')
        self.client = Elasticsearch(["localhost"], port=9205)
        self.doc_type = 'suspension'
        self.domain = "http://sec.gov"
        self.es_index = 'ernest_suspensions'
        self.first_year = 1994
        self.main_sleep = 5
        self.next_year = 2015
        self.page_current = "http://www.sec.gov/litigation/suspensions.shtml"
        self.scrape_sleep = 3
        self.url_fmt = "http://www.sec.gov/litigation/suspensions/suspensionsarchive/susparch{}.shtml"

    def combine_date_links(self, dates, links):
        """ combine url and date into one object """
        return [{"date": dates[i], "link": links[i]} for i in range(0, len(dates))]

    def es_ingest(self, j_obj):
        """ ingest json object into an elasticsearch index """
        self.client.index(index=self.es_index, doc_type=self.doc_type, body=j_obj)

    def get_html(self, url):
        """ get raw html from url """
        response = urlopen(url)
        return response.read()

    def get_pdf(self, pdf_link, pdf_out_loc):
        """ load pdf from file and return release number"""
        response = urlopen(pdf_link)
        with open(pdf_out_loc, 'wb') as outf:
            outf.write(response.read())
        return int(''.join(pdf_link.split('/')[-1:][0].split('-')[:-1]))

    def grab_dates(self, soup_object):
        """ grab all dates from soup and format it before return in date"""
        d = [re.match(self.date_rex, ele.text).group(0) for ele in soup_object.findAll('td') if re.match(self.date_rex, ele.text)]
        return [datetime.strptime(x.replace('.', '').replace(',', ''), "%b %d %Y").strftime('%m-%d-%y') for x in d]

    def grab_links(self, soup_obj, domain):
        """ grab html page and get pdf links from it """
        return [domain + ele for ele in self.link_filter(soup_obj('a', href=True))]

    def link_filter(self, seq):
        """ generator that takes a list of <a> and returns href """
        for ele in seq:
            if '-o.pdf' in ele['href']:
                yield ele['href']

    def main(self):
        i = self.next_year
        self.scrape(self.page_current)

        while i > self.first_year:
            self.page_current = self.url_fmt.format(i)
            self.scrape(self.page_current)
            time.sleep(self.main_sleep)
            i -= 1

    def parse_pdf(self, pdf_location, xml_out_loc):
        """ use pdfquery to parse pdf and write out to xml """
        pdf = pdfquery.PDFQuery(pdf_location,
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
        pdf.tree.write(xml_out_loc)
        return True

    def parse_xml(self, xml_loc):
        """ parse out business names from XML file """
        i = 0
        company_list = []
        c = {"str": "", "flag": False}  # cache dictionary for 2 line names
        soup = self.xml_to_soup(xml_loc)  # convert xml to BS object

        for ele in soup.findAll('LTTextLineHorizontal'):
            m = re.search(self.btypes, ele.text)
            if m:
                n = re.search(self.aka, ele.text)
                if n:
                    if not c['flag'] and len(c['str'].strip()) > 1:
                        company_list.append(
                            c['str'] + " " + n.group(2).strip())
                    else:
                        company_list.append(n.group(2).strip())
                    c['str'] = n.group(2).strip()
                    c['flag'] = True
                else:
                    if not c['flag'] and len(c['str'].strip()) > 1:
                        company_list.append(
                            c['str'] + " " +
                            m.group(1).strip() + " " +
                            m.group(2).strip())
                    else:
                        company_list.append(
                            m.group(1).strip() + " " + m.group(2).strip())
                    c['str'] = m.group(1).strip() + " " + m.group(2).strip()
                    c['flag'] = True
                    i += 1
            else:
                if len(company_list) > 0:
                    break
                c['str'] = ele.text.strip()
                c['flag'] = False

        return company_list

    def scrape(self, get_page):
        """ the full scrape of a page """
        domain = self.domain
        page_text = self.get_html(get_page)
        soup = BeautifulSoup(page_text, 'xml')
        dates = self.grab_dates(soup)
        links = self.grab_links(soup, domain)
        r_obj = self.combine_date_links(dates, links)
        print("Grabbed " + get_page)
        for l in r_obj:
            print("Grabbed " + l['link'])
            release = self.get_pdf(l['link'], '/tmp/sec_temp_file.pdf')
            self.parse_pdf('/tmp/sec_temp_file.pdf', '/tmp/sec_temp_file.xml')
            c_list = self.parse_xml('/tmp/sec_temp_file.xml')
            for c in c_list:
                self.es_ingest({"release_number": release, "link": l['link'], "date": l['date'], "company": c})
            time.sleep(self.scrape_sleep)

    def xml_to_soup(self, xml_loc):
        """ convert xml file into soup object """
        with open('/tmp/sec_temp_file.xml', 'r') as inf:
            x = inf.read()

        soup = BeautifulSoup(x, 'xml')
        return soup

if __name__ == "__main__":
    ScrapeSEC().main()
