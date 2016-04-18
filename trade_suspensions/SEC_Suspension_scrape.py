#!/usr/bin/env python

"""
I don't like writing comment headers so I will keep this one short... she is
not pretty,but she will get you from point A to point B.

This code scrapes suspensions from www.sec.gov and combines it with with a CIK
number file located at www.sec.gov/edgar/NYU/cik.coleft.c using a variant of
Levenshtein Distance. If you follow the steps from the main() fuction, you will
easily see how she works.

The script outputs data in a .csv format. The variable that holds the location
is in the __init__ function and is named self.w_file. The output will look
like:

date, CIK Number, Business Name, Document Containing Info., Levenshtein Score,
Our Business Name.

I included the Levenshtein Score and the business name that we were looking up
on the backside as a way for you to see if our search was right, wrong, or
close.

This script was written on 28 March 2015 by Morgan Peterson on a linux box
using Python 2.7.6.

Use this script as you see fit.

"""


from fuzzywuzzy import process
from selenium import webdriver

# import requests
from urllib.request import urlopen
from bs4 import BeautifulSoup

# import re
# import sys


class ScrapeSEC:
    def __init__(self):
        # "http://sec.gov/litigation/suspensions.shtml"
        self.page_current = "http://www.sec.gov/litigation/suspensions.shtml"
        # http://www.sec.gov/litigation/suspensions/suspensionsarchive/
        # susparch + year + .shtml
        self.write_file = "SEC_Scrape.csv"
        self.CIK_db = {}
        self.wd = webdriver.PhantomJS()

    def link_filter(self, seq):
        """ generator that takes a list of <a> and returns href """
        for ele in seq:
            if '.pdf' in ele['href']:
                yield ele['href']

    def grab_links(self, p_addr):
        """ grab html page and get pdf links from it """
        domain = "http://sec.gov"
        self.wd.get(p_addr)
        page_text = self.wd.page_source
        soup = BeautifulSoup(page_text)

        return [domain + ele for ele in self.link_filter(soup('a', href=True))]

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
        c_cik = ""
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

    def main(self):
        print("Loading cik db...")
        self.load_CIK()
        print("... db loaded!")

s = ScrapeSEC()
s.main()
