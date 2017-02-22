#!/usr/bin/env python

import re
import sys
import json
import argparse

from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from elasticsearch import Elasticsearch

# --
# define CLI

parser = argparse.ArgumentParser(description='omx_html_scrape')
parser.add_argument('--start-page', type=int, action='store')
parser.add_argument('--config-path', type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{
    'host': config["es"]["host"],
    'port': config["es"]["port"]
}], timeout=60000)


# --
# define global vars

""" GLOBAL VARIABLES -- NEED TO MOVE TO CONFIG FILE """
g_re_ticker = re.compile('\w+:\w+')
g_domain = "http://globenewswire.com"
g_home_page = g_domain + "/NewsRoom"
g_out_file = "articles.json"
g_article_cssselector = ".results-link > .post-title16px > a"
g_selenium_wait = 3
g_author = {"name": "author"}
g_title = {"name": "title"}
g_date = {"name": "DC.date.issued"}
g_headline = {"tag": "h1", "attr": "class", "name": "article-headline"}
g_sheadline = {"tag": "h2", "attr": "class",  "name": "subheadline"}
g_article = {"tag": "span", "attr": "itemprop", "name": "articleBody"}
g_ticker = {"name": "ticker"}
g_start_page = args.start_page
g_contacts = {"tag": "pre", "attr": "class", "name": "contactpre"}
g_error_csv = "/home/ubuntu/data/error_logs/omx_html_page_errors.csv",
browser = webdriver.PhantomJS()

# --
# es connection
# --
# define functions


def build_ticker_dict(ticker_array):
    try:
        return {"exchange": ticker_array[0], "symbol": ticker_array[1]}
    except:
        print >> sys.stderr, "-- Unable to build ticker dictionary"
        return {"exchange": None, "symbol": None}


def convert_date(date_string):
    try:
        return datetime.strptime(
            date_string, "%m/%d/%Y").strftime("%Y-%m-%d")
    except:
        print >> sys.stderr, "-- Unable to parse date string"


def get_page_soup(url):
    """ render js to html and return it from webdriver """
    browser.get(url)
    try:
        WebDriverWait(browser, g_selenium_wait).until(
                        EC.presence_of_element_located((By.ID, "articleBody")))
    finally:
        return BeautifulSoup(browser.page_source.encode('utf-8', 'ignore'))


def get_tags(soup):
    try:
        ahrefs = soup.findAll('a', {"class": "article_tag"})
        return [a.text for a in ahrefs]
    except:
        print >> sys.stderr, "-- Unable to acquire tags"


def get_links(soup):
    """ grab article links from one page """
    try:
        links = soup.select(g_article_cssselector)
        return [k.get('href') for k in links]
    except:
        print >> sys.stderr, "-- Unable to get article links from page"


def split_tickers(ticker_string):
    """ split a string of ticker symbols into a list of ticker
        symbols
    """
    try:
        tmp = g_re_ticker.findall(ticker_string)
        return [build_ticker_dict(t.split(":")) for t in tmp]
    except:
        print >> sys.stderr, "-- Unable to split tickers"


def meta_handler(g_var, soup_object):
    error_msg = "Meta tag doesn't exist [{}]"
    try:
        return soup_object.find(attrs={"name": g_var['name']})['content']
    except:
        print(error_msg.format(g_var['name']))


def article_handler(g_var, soup_object):
    error_msg = "Article element not found [{}]"
    try:
        return soup_object.find(
            g_var['tag'], {g_var['attr']: g_var['name']}).text
    except:
        print(error_msg.format(g_var['name']))


def get_contact(g_var, soup_object):
    links = None
    error_msg = "Contact information not found [{}]"
    try:
        contact = soup_object.find(
            g_var['tag'], {g_var['attr']: g_var['name']})
        links = [l.text for l in contact.findAll('a', href=True)]
        paragraphs = contact.text.split('\n\n')
        return {"links": links, "contacts": paragraphs}
    except:
        print(error_msg.format(g_var['name']))
        return {"links": None, "contacts": None}


def msg_exists(our_id):
    ''' Check if newswire already exists in elasticsearch '''
    try:
        _ = client.get(
            index=config['omx']['index'],
            doc_type=config['omx']['_type'],
            id=our_id)
        return True
    except:
        return False


def parse_article(soup, url, url_id):
    """ Parse an article into a dictionary object that will be
        passed to an elasticsearch instance """
    return {
        "url": url,
        "id": url_id,
        "tickers": split_tickers(meta_handler(g_ticker, soup)),
        "author": meta_handler(g_author, soup),
        "title": meta_handler(g_title, soup),
        "date": convert_date(meta_handler(g_date, soup)),
        "headline": article_handler(g_headline, soup),
        "subheadline": article_handler(g_sheadline, soup),
        "article": article_handler(g_article, soup),
        "company": get_company_info(soup),
        "tags": get_tags(soup)
    }


def apply_function(link):
    link_id = link.split('/')[7]
    s = get_page_soup(link)
    return parse_article(s, link, link_id)


def parse_page(page_domain, full_page_html):
    """ main function for parsing articles from a single page """
    print >> sys.stderr, '-- making the soup'
    soup = get_page_soup(full_page_html)
    print >> sys.stderr, '-- got the soup'
    links = [page_domain + link for link in get_links(soup)]
    print >> sys.stderr, '-- Processing %s links' % len(links)
    print('-- Processing {} link(s) --'.format(len(links)))
    articles = [apply_function(link) for link in links if not msg_exists(link.split('/')[7])]
    print >> sys.stderr, "-- %s article(s) to be indexed" % len(articles)
    print("-- {} article(s) to be indexed --".format(len(articles)))
    for article in articles:
        try:
            client.index(
                index=config['omx']['index'],
                doc_type=config['omx']['_type'],
                id=article["id"],
                body=article
            )
        except:
            browser.quit()
            raise


def get_company_info(soup):
    info = {"company": None, "symbol": None, "location": None}
    st_info = {}
    try:
        raw_list = soup.find(
            'div', {"id": "stockInfoContainer"}).find('strong').text.split('(')
        info['company'] = raw_list[0]
    except:
        print >> sys.stderr, "-- No company name in text --"
    try:
        info['symbol'] = raw_list[1].split(':')[1][:-1]
    except:
        print >> sys.stderr, "-- No symbol in text --"
    try:
        info['location'] = soup.find(
            'p', {'itemprop': 'dateline contentLocation'}).text.strip()
    except:
        print >> sys.stderr, "-- No location available --"
    info['contact'] = get_contact(g_contacts, soup)
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


def main():
    max_pages = g_start_page
    # url_fmt = 'http://globenewswire.com/NewsRoom?page={}'
    for i in range(max_pages, 0, -1):
        article_url = 'http://globenewswire.com/NewsRoom?page={}'.format(i)
        print >> sys.stderr, '%s :: %s' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), article_url)
        print('{0} :: {1}'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), article_url))
        parse_page(g_domain, article_url)
        print >> sys.stderr, ''
        print


# --
# run
if __name__ == "__main__":
    main()
    browser.quit()
