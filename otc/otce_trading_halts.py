import urllib2
import requests
import csv
import re
import time
import argparse

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

INDEX  = config['otc']['halts']['index']
TYPE   = config['otc']['halts']['_type']

# -- 
# configure driver

display = Display(visible=0, size=(800, 600))
display.start()

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
                'dateTime'      : facts[0].get_text(),
                'ticker'        : facts[1].get_text(), 
                'issuerName'    : facts[2].get_text(),
                'haltCode'      : facts[3].get_text(),
                'mktCtrOrigin'  : facts[4].get_text(),
                'Action'        : facst[5].get_text(),
                'secHalt'       : sec_halt(facts)
            }
            client.index(index = INDEX, doc_type = TYPE, \
                body = out, id = out['dateTime'] + '_' + out['ticker']) 
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
                'dateTime'      : facts[0].get_text(),
                'ticker'        : facts[1].get_text(), 
                'issuerName'    : facts[2].get_text(),
                'haltCode'      : facts[3].get_text(),
                'mktCtrOrigin'  : facts[4].get_text(),
                'Action'        : facst[5].get_text(),
                'secHalt'       : sec_halt(facts)
            }
            client.index(index = INDEX, doc_type = TYPE, \
                body = out, id = out['ticker'] + '_' + out['ticker']) 
        msg      = 'bad'
        counter += 1


driver.quit()