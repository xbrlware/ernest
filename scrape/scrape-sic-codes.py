#!/usr/bin/env python

import pickle
import urllib2 
from bs4 import BeautifulSoup

# -- 
# define vars 

outpath  = "/home/ubuntu/data/sic_codes/sic_ref.p"
url = 'https://www.sec.gov/info/edgar/siccodes.htm'

# --
# function

def get_refs(sics):
    ref = {}
    for i in range(0, len(sics)): 
        facts = sics[i].findAll('td')
        if len(facts) == 4: 
            sic   = facts[0].get_text()
            text  = facts[3].get_text()
            ref[sic] = text
        else: 
            pass
        
    return ref


# --
# run 

if __name__ == "__main__":
    sic  = BeautifulSoup(urllib2.urlopen(url))
    sics = sic.findAll('tr', {'valign' : ['top']})
    sic_dict = get_refs(sics)
    pickle.dump(sic_dict, open(outpath, "wb"))