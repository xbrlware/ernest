#!/usr/bin/python

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup

import datetime as dt
import feedparser
import json
import re
import sys
import time
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# --
# instatiate browser

browser = webdriver.PhantomJS()

# --
# define CLI

parser = argparse.ArgumentParser(description='omx_html_scrape')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

# --
# config

config = json.load(open(args.config_path))


# -- 
# es connection 

client = Elasticsearch([{
    'host' : config["es"]["host"],
    'port' : config["es"]["port"]
}], timeout = 60000)



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



# --
# define functions

def meta_handler(g_var, soup_object):
    error_msg = "Meta tag doesn't exist [{}]"
    try:
        x = soup_object.find(
                attrs={"name": g_var['name']})['content']
    except:
        print(error_msg.format(g_var['name']))
        return None

    if len(x) < 1:
        return None

    return x


def article_handler(g_var, soup_object):
    error_msg = "Article element not found [{}]"
    try:
        x = soup_object.find(
                g_var['tag'], {g_var['attr']: g_var['name']}).text
    except:
        print(error_msg.format(g_var['name']))
        return None

    if len(x) < 1:
        x = None

    return x


def convert_date(date_string):
    try:
        date_obj = dt.datetime.strptime(date_string, "%m/%d/%Y")
    except:
        print("Unable to parse date string")
        return None

    return date_obj.strftime("%Y-%m-%d")


def build_ticker_dict(ticker_array):
    try:
        return {"exchange": ticker_array[0],
                "symbol": ticker_array[1]}
    except:
        print("Unable to build ticker dictionary")
        return {"exchange": None, "symbol": None}


def split_tickers(ticker_string):
    """ split a string of ticker symbols into a list of ticker
        symbols
    """
    try:
        temp = g_re_ticker.findall(ticker_string)
    except:
        print("Unable to split tickers")
        return None

    if len(temp) < 1:
        return None
    else:
        return [build_ticker_dict(t.split(":")) for t in temp]


def get_company_info(soup):
    info = {}
    st_info = {}
    try:
        raw_list = soup.find(
                'div', {"id": "stockInfoContainer"}).find(
                        'strong').text.split('(')
        info['company'] = raw_list[0]
        info['symbol'] = raw_list[1].split(':')[1][:-1]
    except:
        print("No company name and symbol")
        info['company'] = None
        info['symbol'] = None

    try:
        info['location'] = soup.find(
                'p',
                {'itemprop':
                    'dateline contentLocation'}).text.strip()
    except:
        print("No location available")
        info['location'] = None

    info['contact'] = get_contact(
            {"tag": "pre",
                "attr": "class", "name": "contactpre"}, soup)

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


def get_contact(g_var, soup_object):
    links = []
    error_msg = "Contact information not found [{}]"
    try:
        contact = soup_object.find(
                g_var['tag'], {g_var['attr']: g_var['name']})
        temp_links = contact.findAll('a', href=True)

        links = [l.text for l in temp_links]

        paragraphs = contact.text.split('\n\n')
    except:
        print(error_msg.format(g_var['name']))
        return {"links": None, "contacts": None}

    if len(links) < 1:
        links = None

    return {"links": links, "contacts": paragraphs}


def get_tags(soup):
    try:
        ahrefs = soup.findAll('a', {"class": "article_tag"})
    except:
        print("Unable to acquire tags")
        return None

    if len(ahrefs) < 1:
        return None
    else:
        return [a.text for a in ahrefs]


def parse_article(soup, url):
    """ Parse an article into a dictionary object that will be
        passed to an elasticsearch instance """
    article_obj = {}
    article_obj['url'] = url
    article_obj['id'] = url.split('/')[7]

    temp_ticker = meta_handler(g_ticker, soup)

    if temp_ticker and len(temp_ticker) > 0:
        article_obj['tickers'] = split_tickers(temp_ticker)
    else:
        article_obj['tickers'] = None

    article_obj['author'] = meta_handler(g_author, soup)
    article_obj['title'] = meta_handler(g_title, soup)

    raw_date = meta_handler(g_date, soup)
    article_obj['date'] = convert_date(raw_date)

    article_obj['headline'] = article_handler(g_headline, soup)
    article_obj['subheadline'] = article_handler(g_sheadline, soup)
    article_obj['article'] = article_handler(g_article, soup)
    article_obj['company'] = get_company_info(soup)
    article_obj['tags'] = get_tags(soup)

    return article_obj


def get_page_src(url):
    """ render js to html and return it from webdriver """
    browser.get(url)
    try:
        unused = WebDriverWait(
                browser, g_selenium_wait).until(
                        EC.presence_of_element_located(
                            (By.ID, "articleBody")))
    finally:
        return browser.page_source


def get_soup(page_source):
    """ return soup object from raw html """
    return BeautifulSoup(page_source)


def get_articles(link_list):
    """ loops throught a list of article links returned from
        feedparser, parses them into a list of dict's ready from
        ingest into ES
    """
    body = []
    for link in link_list:
        print("Getting {}... ".format(link))
        page = get_page_src(link)
        print("Making soup... ")
        soup = get_soup(page.encode('utf-8', 'ignore'))
        print("Getting article content... ")
        cont = parse_article(soup, link)
        print("Appending to list... ")
        body.append(cont)
    return body


def get_links(feed, gmt_time):
    """ grab article links from rss feed """
    links = []
    for entry in feed.entries:
        if entry.updated_parsed > gmt_time:
            links.append(entry.link)

    return links


def get_countries():
    """ get list of all the country RSS feeds """
    r = []
    domain = "http://globenewswire.com"
    url = "http://globenewswire.com/Rss/List"
    browser.get(url)
    soup = BeautifulSoup(browser.page_source)
    try:
        all_links = soup.findAll('a', {"title": "RSS Feed"})
    except:
        print("Could not find RSS feed links")
        print("Scraper Exiting!")
        sys.exit(1)

    for link in all_links:
        if link.text == 'RSS':
            if 'country' in link['href']:
                r.append(domain + link['href'])

    return r


def article_soup(links):
    if len(links) < 0:
        print("No links to update")

    print("Getting articles...")
    return get_articles(links)


def main():
    urls = get_countries()
    gmt_time = [None for u in urls]

    print("Feeds: {}".format(urls))

    while True:
        i = 0
        for u in urls:
            print("Getting feed... ")
            feed = feedparser.parse(u)
            fd_time = feed.entries[0].updated_parsed

            if gmt_time[i] < fd_time or gmt_time[i] is None:
                print("Getting links... ")
                links = get_links(feed, gmt_time[i])
                artic = article_soup(links)

                print("Writing to file...")
                for a in artic:
                    client.index(index = config['omx']['index'], doc_type = config['omx']['_type'], body = a, id = a["id"])

                gmt_time[i] = feed.entries[0].updated_parsed
            else:
                print("Feed has not been updated")
            i += 1
            time.sleep(10)


# --
# run 

if __name__ == "__main__":
    main()

driver.quit() 
