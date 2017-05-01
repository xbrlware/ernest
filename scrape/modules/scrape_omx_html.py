#!/usr/bin/env python

import argparse
import json
import logging
import multiprocessing
import re
import time
import requests

from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
from enrich_modules.enrich_to_cik import TO_CIK


class OMX_SCRAPER:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + '.omx_html_scraper')
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            'host': config["es"]["host"],
            'port': config["es"]["port"]
        }], timeout=60000)

        self.g_re_ticker = re.compile('\w+:\w+')
        self.g_domain = "http://globenewswire.com"
        self.g_home_page = self.g_domain + "/NewsRoom"
        self.g_out_file = "articles.json"
        self.g_article_cssselector = ".results-link > .post-title16px > a"
        self.g_selenium_wait = 3
        self.g_author = {"name": "author"}
        self.g_title = {"name": "title"}
        self.g_date = {"name": "DC.date.issued"}
        self.g_headline = {"tag": "h1", "attr": "class",
                           "name": "article-headline"}
        self.g_sheadline = {"tag": "h2", "attr": "class",
                            "name": "subheadline"}
        self.g_article = {"tag": "span", "attr": "itemprop",
                          "name": "articleBody"}
        self.g_ticker = {"name": "ticker"}
        self.g_start_page = args.start_page
        self.g_contacts = {"tag": "pre", "attr": "class", "name": "contactpre"}
        self.g_user_agent = "Mozilla/5.0 (X11; Linux x86_64) \
            AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu \
            Chromium/56.0.2924.76 Chrome/56.0.2924.76 Safari/537.36"
        self.g_requests_header = {"User-Agent": self.g_user_agent}

    def build_ticker_dict(self, ticker_array):
        try:
            return {"exchange": ticker_array[0], "symbol": ticker_array[1]}
        except:
            self.logger.warn("Unable to build ticker dictionary")
            return {"exchange": None, "symbol": None}

    def convert_date(self, date_string):
        try:
            return datetime.strptime(
                date_string, "%m/%d/%Y").strftime("%Y-%m-%d")
        except:
            self.logger.warn("Unable to parse date string")

    def get_page_soup(self, url):
        """ render js to html and return it from requests """
        page = requests.get(url, headers=self.g_requests_header, timeout=30)
        return BeautifulSoup(page.content)

    def get_tags(self, soup):
        try:
            ahrefs = soup.findAll('a', {"class": "article_tag"})
            return [a.text for a in ahrefs]
        except:
            self.logger.warn("Unable to acquire tags")

    def get_links(self, soup):
        """ grab article links from one page """
        try:
            links = soup.select(self.g_article_cssselector)
            return [k.get('href') for k in links]
        except:
            self.logger.warn("Unable to get article links")

    def split_tickers(self, ticker_string):
        """ split a string of ticker symbols into a list of ticker symbols"""
        try:
            tmp = self.g_re_ticker.findall(ticker_string)
            return [self.build_ticker_dict(t.split(":")) for t in tmp]
        except:
            self.logger.warn("Unable to split tickers")

    def meta_handler(self, g_var, soup_object):
        error_msg = "Meta tag doesn't exist [{}]"
        try:
            return soup_object.find(attrs={"name": g_var['name']})['content']
        except:
            self.logger.error('{}'.format(error_msg.format(g_var['name'])))

    def article_handler(self, g_var, soup_object):
        error_msg = "Article element not found [{}]"
        try:
            return soup_object.find(
                g_var['tag'], {g_var['attr']: g_var['name']}).text
        except:
            self.logger.error('{}'.format(error_msg.format(g_var['name'])))

    def get_contact(self, g_var, soup_object):
        links = None
        error_msg = "Contact information not found [{}]"
        try:
            contact = soup_object.find(
                g_var['tag'], {g_var['attr']: g_var['name']})
            links = [l.text for l in contact.findAll('a', href=True)]
            paragraphs = contact.text.split('\n\n')
            return {"links": links, "contacts": paragraphs}
        except:
            self.logger.error('{}'.format(error_msg.format(g_var['name'])))
            return {"links": None, "contacts": None}

    def msg_exists(self, our_id):
        ''' Check if newswire already exists in elasticsearch '''
        try:
            self.client.get(
                index=self.config['omx']['index'],
                doc_type=self.config['omx']['_type'],
                id=our_id)
            return True
        except:
            return False

    def parse_article(self, soup, url, url_id):
        """ Parse an article into a dictionary object that will be
            passed to an elasticsearch instance """
        return {
            "url": url,
            "id": url_id,
            "tickers": self.split_tickers(self.meta_handler(
                self.g_ticker, soup)),
            "author": self.meta_handler(self.g_author, soup),
            "title": self.meta_handler(self.g_title, soup),
            "date": self.convert_date(self.meta_handler(self.g_date, soup)),
            "headline": self.article_handler(self.g_headline, soup),
            "subheadline": self.article_handler(self.g_sheadline, soup),
            "article": self.article_handler(self.g_article, soup),
            "company": self.get_company_info(soup),
            "tags": self.get_tags(soup)
        }

    def article_thread(self, links):
        i = 0
        for link in links:
            if not self.msg_exists(link.split('/')[7]):
                article = self.apply_function(link)
                try:
                    self.client.index(
                        index=self.config['omx']['index'],
                        doc_type=self.config['omx']['_type'],
                        id=article["id"],
                        body=article
                    )
                    i += 1
                except:
                    raise

            time.sleep(4)
        self.logger.info("{} articles indexed!".format(i))

    def apply_function(self, link):
        link_id = link.split('/')[7]
        s = self.get_page_soup(link)
        return self.parse_article(s, link, link_id)

    def parse_page(self, page_domain, full_page_html):
        """ main function for parsing articles from a single page """
        soup = self.get_page_soup(full_page_html)
        links = [page_domain + link for link in self.get_links(soup)]
        self.article_thread(links)

    def get_company_info(self, soup):
        info = {"company": None, "symbol": None, "location": None}
        st_info = {}
        try:
            raw_list = soup.find(
                'div',
                {"id": "stockInfoContainer"}).find('strong').text.split('(')
            info['company'] = raw_list[0]
        except:
            self.logger.warn("No company name in text")
        try:
            info['symbol'] = raw_list[1].split(':')[1][:-1]
        except:
            self.logger.warn("No symbol in text")
        try:
            info['location'] = soup.find(
                'p', {'itemprop': 'dateline contentLocation'}).text.strip()
        except:
            self.logger.warn("No location available")
        info['contact'] = self.get_contact(self.g_contacts, soup)
        for f in soup.findAll('div', {"class": "stockdivider"}):
            x = f.find('p').text.strip().split('\n')
            y1 = x[0].split()
            try:
                st_info[y1[1][:-1]] = x[1].strip()
            except:
                st_info[y1[0][:-1]] = x[1].strip()
        if not st_info:
            st_info = None
        info['stock'] = st_info
        return info

    def main(self):
        jobs = []
        max_pages = self.g_start_page
        for i in range(max_pages, 0, -1):
            article_url = 'http://globenewswire.com/NewsRoom?page={}'.format(i)
            self.logger.info('{}'.format(article_url))
            p = multiprocessing.Process(
                target=self.parse_page,
                args=(self.g_domain, article_url,)
            )
            jobs.append(p)
            p.start()
            time.sleep(2)

        # stops main from crashing so enrichment can happen in another script
        for job in jobs:
            job.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='omx_scraper')
    parser.add_argument('--start-page', type=int, action='store')
    parser.add_argument('--config-path', type=str, action='store')
    parser.add_argument('--halts',
                        dest='halts',
                        action="store_true")
    parser.add_argument("--index",
                        type=str, action='store',
                        required=True)
    parser.add_argument("--ticker-to-cik-field-name",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    args = parser.parse_args()

    omxs = OMX_SCRAPER(args)
    to_cik = TO_CIK(args, 'omx_scraper')

    omxs.main()
    to_cik.ticker_to_cik()
