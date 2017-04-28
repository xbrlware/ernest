#!/usr/bin/env python3

import re
# import requests

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


url_base = "https://finance.yahoo.com"
url_main = url_base + "/news/provider-prnewswire/?bypass=true"
main_page_ele = 'li'
main_page_class = 'nothumb'
re_tick = re.compile(r"\/q\?s\=[a-z]{4}\.{0,1}[a-z]{0,2}")
browser = webdriver.Chrome('/usr/bin/chromedriver')


def get_page_src(url, element):
    """ render js to html and return it from webdriver """
    wait_t = 3
    browser.get(url)
    try:
        WebDriverWait(
                browser, wait_t).until(
                        EC.presence_of_element_located(
                            (By.CLASS, element)))
    finally:
        return browser.page_source


def get_soup(um, element):
    return BeautifulSoup(get_page_src(um, element), 'lxml')


def get_links(ub, um, le, lc):
    soup = get_soup(um, 'yom-top-story-content-0')
    return [ub + h.a['href'] for h in soup.find_all(le, class_=lc)]


def find_element(element, lookup, func):
    r = func(element, attrs=lookup)
    if isinstance(r, str):
        return r.text
    else:
        return r


def get_release(links):
    for link in links:
        soup = get_soup(link, 'content-canvas')
        h1 = find_element('h1', {'itemprop': 'headline'}, soup.find)
        # h1 = soup.find({'h1', itemprop="headline"})
        pv = find_element('a', {'itemprop': 'url'}, soup.find)
        # pv = soup.find('a', itemprop="url")
        tm = find_element('time', {'class': 'date'}, soup.find)

        # tm = soup.find('time', class_="date")
        # ps = soup.find_all('p', class_="canvas-atom")
        try:
            au = soup.find('a', id="PRNURL")['href']
        except TypeError as e:
            au = None
            print('TYPEERROR| {}'.format(e))

        try:
            tickers = find_element('a', {'href': re_tick}, soup.find_all)
            # tickers = soup.find('a', href=re_tick).text
        except AttributeError:
            tickers = None

        """
        for ele in ps[:-2]:
            print(ele.text.strip())
            print('\n')
        """
        print((tickers, h1, pv, tm, au))


get_release(get_links(url_base, url_main, main_page_ele, main_page_class))
browser.quit()
