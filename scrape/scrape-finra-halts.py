import urllib2
import requests
import csv
import re
import time
import argparse
import json

from bs4 import BeautifulSoup
from pprint import pprint
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

from pyvirtualdisplay import Display

# --
# CLI

parser = argparse.ArgumentParser(description='ingest_otc_halts')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

# --
# config

# config      = json.load(open(args.config_path))

config      = json.load(open('/home/ubuntu/ernest/config.json'))


# --
# connections

client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])

INDEX  = config['otc_halts']['index']
TYPE   = config['otc_halts']['_type']


# display = Display(visible=0, size=(800, 600))
# display.start()

driver = webdriver.PhantomJS() 
driver.get('http://otce.finra.org/TradeHaltsHistorical')


# -- 
# helpers

def sec_halt(facts): 
    try: 
        facts[6].find('a').get_text()
        sec_halt = True
    except: 
        sec_halt = False
    return sec_halt

def build_date( date ): 
    f = re.sub(r'/', '&', date)
    d = re.compile("\d{2}&\d{2}&\d{4}")
    c = re.findall(d, f)[0].split('&')
    out_d = c[2] + '-' + c[0] + '-' + c[1]
    return out_d

# -- 
# run

counter = 0
while True:
    time.sleep(6)
    posts = BeautifulSoup(driver.page_source).findAll("tr", {'class' : ['odd', 'even']})  
    time.sleeo(3)
    for post in posts: 
        facts = post.findAll('td')
        time.sleep(3)
        out   = {
            'dateTime'      : build_date( facts[0].get_text() ),
            'ticker'        : facts[1].get_text().upper(), 
            'issuerName'    : facts[2].get_text().upper(),
            'haltCode'      : facts[3].get_text(),
            'mktCtrOrigin'  : facts[4].get_text(),
            'Action'        : facts[5].get_text(),
            'secHalt'       : sec_halt(facts)
        }
        client.index(index=INDEX, doc_type=TYPE, body=out, id=out['dateTime'] + '_' + out['ticker']) 
        
    counter += 1
    print(counter)

    try:
        driver.find_element_by_xpath("//*[contains(text(), 'Next')]").click()
    except:
        break

driver.quit()
