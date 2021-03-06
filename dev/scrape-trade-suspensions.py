#!/usr/bin/env python

import json
import re
import pdfquery
import urllib

from fuzzywuzzy import process
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.request import urlopen

class ScrapeSEC:
    
    page_current = "http://www.sec.gov/litigation/suspensions.shtml"
    CIK_db       = {}
    
    def __init__(self):
        self.wd     = webdriver.PhantomJS()
        self.btypes = re.compile('(.*)(Inc|INC|Llc|LLC|Comp|COMP|Ltd|LTD|Corp|CORP)(\.*)')
        self.aka    = re.compile('(\(*./k/a\)*) ([A-Za-z\.\, ]+)')

    def get_pdf(self, pdf_link, pdf_out_loc):
        response = urllib.request.urlopen(pdf_link)
        open(pdf_out_loc, 'wb').write(response.read())
        return True

    def parse_pdf(self, pdf_location, xml_out_loc):
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

    def xml_to_soup(self, xml_loc):
        """ convert xml file into soup object """
        x = open('/tmp/todd.xml', 'r').read()
        return BeautifulSoup(x, 'xml')

    def parse_xml(self, xml_loc):
        """ parse out business names from XML file """
        i = 0
        company_list = []
        c = {"str": "", "flag": False}  # cache dictionary for 2 line names
        soup = self.xml_to_soup(xml_loc)  # convert xml to BS object

        for ele in soup.findAll('LTTextLineHorizontal'):
            m = re.search(self.btypes, ele.text)
            """ if search for a company name in the line is true
                    if there was a previous line with text and text
                        length is greater than one on that line prepend that"""
            if m:
                n = re.search(self.aka, ele.text)
                if n:
                    if not c['flag'] and len(c['str'].strip()) > 1:
                        company_list.append('%s %s' % (c['str'], n.group(2).strip()))
                    else:
                        company_list.append(n.group(2).strip())
                    
                    c['str']  = n.group(2).strip()
                    c['flag'] = True
                else:
                    if not c['flag'] and len(c['str'].strip()) > 1:
                        company_list.append('%s %s %s' % (c['str'],  m.group(1).strip(),  m.group(2).strip()))
                    else:
                        company_list.append('%s %s' % (m.group(1).strip(), m.group(2).strip()))
                        
                    c['str'] = '%s %s' % (m.group(1).strip(), m.group(2).strip())
                    c['flag'] = True
                    
                    i += 1
            else:
                if len(company_list) > 0:
                    break
                c['str'] = ele.text.strip()
                c['flag'] = False

        return company_list

    def link_filter(self, seq):
        """ generator that takes a list of <a> and returns href """
        for ele in seq:
            if '-o.pdf' in ele['href']:
                yield ele['href']

    def grab_dates(self, soup_object):
        """ grab all dates from html page object """
        date_rex = re.compile('[JFMASOND][aepuco][nbrynlgptvc]\.{0,1} [0-3][0-9], 20[0-1][0-6]')
        return [re.match(date_rex, ele.text).group(0) for ele in soup_object.findAll('td') if re.match(date_rex, ele.text)]

    def grab_links(self, soup_obj, domain):
        """ grab html page and get pdf links from it """
        return [domain + ele for ele in self.link_filter(soup_obj('a', href=True))]

    def load_CIK(self):
        """ loads in the CIK file from the sec site """
        # self.CIK_loc
        CIK_loc = "https://www.sec.gov/edgar/NYU/cik.coleft.c"
        for line in urlopen(CIK_loc):
            l = str(line).split(':')[:-1]
            self.CIK_db[l[0]] = l[1]

        return True

    def search_CIKDB(self, s_name):
        """ searches through the list of CIK and returns a list of
            [cik, cik score, company name, incoming name]
        """
        c_cik  = ""
        c_name = ""
        u_name = s_name.upper()

        if u_name in self.CIK_db.keys():
            return [self.CIK_db[u_name], 0, u_name, u_name]
        else:
            cik_name_list = self.CIK_db.keys()
            match = process.extractOne(u_name, cik_name_list)
            c_name = match[0]
            c_cik = self.CIK_db[c_name]
            c_score = match[1]

            return [c_cik, c_score, c_name, u_name]

    def combine_date_links(self, dates, links):
        r_obj = []
        for i in range(0, len(dates)):
            r_obj.append({"date": dates[i], "link": links[i]})

        return r_obj

    def main(self, get_page):
        # final_obj = []
        # cik = []
        # print("Loading cik db...")
        # self.load_CIK()
        # print("... db loaded!")
        print("Grabbing home page links...")
        domain = "http://sec.gov"
        self.wd.get(get_page)
        page_text = self.wd.page_source
        soup  = BeautifulSoup(page_text, 'xml')
        dates = self.grab_dates(soup)
        links = self.grab_links(soup, domain)
        r_obj = self.combine_date_links(dates, links)
        print("... links grabbed!")
        with open('tester.json', 'a') as outf:
            for l in r_obj:
                print("Get pdf...")
                self.get_pdf(l['link'], '/tmp/todd.pdf')
                print("... got pdf.")
                print("Parsing pdf... ")
                self.parse_pdf('/tmp/todd.pdf', '/tmp/todd.xml')
                print("... Done parsing pdf")
                print("Parse xml ... ")
                c_list = self.parse_xml('/tmp/todd.xml')
                for c in c_list:
                    json.dump({"cik": 0, "link": l['link'], "date": l['date'], "company": c}, outf)
                    outf.write('\n')
                    print({"cik": 0, "link": l['link'], "date": l['date'], "company": c})
                
                print("... xml parsed")

s = ScrapeSEC()
i = 2015
s.main('http://www.sec.gov/litigation/suspensions/suspensionsarchive/susparch2014.shtml')
# s.main(s.page_current)

"""
while i > 1994:
    i -= 1
    s.page_current = "http://www.sec.gov/litigation/suspensions/suspensionsarchive/susparch{}.shtml".format(i)
    s.main(s.page_current)

"""
