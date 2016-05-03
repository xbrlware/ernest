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

#--
# config
config_path = args.config_path
config      = json.load(open(config_path))

# --
# es connection
client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])

INDEX  = config['otc']['directory']['index']
TYPE   = config['otc']['directory']['_type']

# -- 
# configure driver

display = Display(visible=0, size=(800, 600))
display.start()

driver = webdriver.PhantomJS() 
driver.get('http://otce.finra.org/Directories')


# --
# run 

counter = 0
msg     = 'good'
while msg == 'good':
    try: 
        html     = driver.page_source
        soup     = BeautifulSoup(html)
        posts    = soup.findAll("tr", {'class' : ['odd', 'even']})  
        driver.find_element_by_xpath("//*[contains(text(), 'Next')]")  
        for i in posts: 
            facts = i.findAll('td')
            out   = {
                'ticker'      : facts[0].get_text(), 
                'issuerName'  : facts[1].get_text(),
                'market'      : facts[2].get_text(),
                'issuerType'  : facts[3].get_text()
            }
            client.index(index = INDEX, doc_type = TYPE, \
                body = out, id = out['ticker'] + '_' + out['issuerName'] + \
                           '_' + out['market'] + '_' + out['issuerType'] ) 
        msg      = 'good'
        counter += 1
        print(counter)
        driver.find_element_by_xpath("//*[contains(text(), 'Next')]").click()
        time.sleep(1.5)
    except: 
        html     = driver.page_source
        soup     = BeautifulSoup(html)
        posts    = soup.findAll("tr", {'class' : ['odd', 'even']})        
        for i in posts: 
            facts = i.findAll('td')
            out   = {
                'ticker'      : facts[0].get_text(), 
                'issuerName'  : facts[1].get_text(),
                'market'      : facts[2].get_text(),
                'issuerType'  : facts[3].get_text()
            }
            client.index(index = INDEX, doc_type = TYPE, \
                body = out, id = out['ticker'] + '_' + out['issuerName'] + \
                           '_' + out['market'] + '_' + out['issuerType'] ) 
        msg      = 'bad'
        counter += 1


driver.quit()