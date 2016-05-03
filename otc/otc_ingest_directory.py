import urllib2
import requests
import csv
import re
import time

from bs4 import BeautifulSoup
from pprint import pprint
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By

from pyvirtualdisplay import Display


# -- 
# configure driver

display = Display(visible=0, size=(800, 600))
display.start()


driver = webdriver.Chrome('/home/ubuntu/divers/chromedriver') 
driver.get('http://otce.finra.org/Directories')

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
            client.index(index = 'ernest_otc_directory', doc_type = 'reference', \
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
            client.index(index = 'ernest_otc_directory', doc_type = 'reference', \
                body = out, id = out['ticker'] + '_' + out['issuerName'] + \
                           '_' + out['market'] + '_' + out['issuerType'] ) 
        msg      = 'bad'
        counter += 1


driver.close()