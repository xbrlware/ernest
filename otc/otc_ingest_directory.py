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

config = json.load(open(args.config_path))
INDEX  = config['otc']['directory']['index']
TYPE   = config['otc']['directory']['_type']

START_URL = 'http://otce.finra.org/Directories'

# --
# Connections

client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])

display = Display(visible=0, size=(800, 600))
display.start()

driver = webdriver.PhantomJS() 
driver.get(START_URL)

# --
# Run

''' This should work but needs testing - BKJ '''

field_names = ['ticker', 'issuerName', 'market', 'issuerType']

counter = 0
while True:
    time.sleep(1.5)
    posts = BeautifulSoup(driver.page_source).findAll("tr", {'class' : ['odd', 'even']})  
    try: 
        driver.find_element_by_xpath("//*[contains(text(), 'Next')]")  
    except:
        break
    
    for post in posts: 
        facts = [p.get_text() for p in post.findAll('td')]        
        out   = dict(zip(field_names, facts))
        client.index(index=INDEX, doc_type=TYPE, body=out, id='_'.join(facts)) 
        
    counter += 1
    print(counter)

    try:
        driver.find_element_by_xpath("//*[contains(text(), 'Next')]").click()
    except:
        break


driver.quit()