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

parser = argparse.ArgumentParser(description='ingest_otc')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

# -- 
# config

config    = json.load(open(args.config_path))
INDEX     = config['otc']['directory']['index']
TYPE      = config['otc']['directory']['_type']


# --
# global vars

START_URL = 'http://otce.finra.org/Directories'


# --
# connections

client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])

display = Display(visible=0, size=(800, 600))
display.start()

driver = webdriver.PhantomJS() 
driver.get(START_URL)


# --
# run

''' This should work but needs testing - BKJ '''


counter = 0
while True:
    time.sleep(6)
    posts = BeautifulSoup(driver.page_source).findAll("tr", {'class' : ['odd', 'even']})  

    for post in posts: 
        facts = post.findAll('td')
        out   = {
            'ticker'      : facts[0].get_text().upper(), 
            'issuerName'  : facts[1].get_text().upper(),
            'market'      : facts[2].get_text(),
            'issuerType'  : facts[3].get_text()
        }
        client.index(index=INDEX, doc_type=TYPE, body=out, \
                     id=out['ticker'] + '_' + out['issuerName'] + \
                  '_' + out['market'] + '_' + out['issuerType'] ) ) 
        
    counter += 1
    print(counter)

    try:
        driver.find_element_by_xpath("//*[contains(text(), 'Next')]").click()
    except:
        break


driver.quit()