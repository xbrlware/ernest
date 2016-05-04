import re
import os
import csv
import json
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# --
# es connection

client = Elasticsearch([{
    'host' : 'localhost', #config["es"]["host"], 
    'port' : 9205 #config["es"]["port"]
}], timeout = 60000)


# --
# funtion 

def __ingest(year, month): 
    path   = '/home/ubuntu/xbrl/' + year + '/' + month + '/parsed'
    for x in os.listdir(path):
        try: 
            doc    = path + '/' + x
            f      = open(doc, 'rU') 
            reader = csv.reader(f)
            rows   = list(reader)
            entry  = {"link" : doc,
                  "year" : year,
                  "tags" : {}
            }
            for i in range(0, len(rows)): 
                body_set   = [1, 3, 4, 6, 13, 14, 16, 17, 20, 24, 34, 35]
                body_keys  = [rows[0][k -1] for k in body_set]
                y          = rows[i]
                if len(rows[i]) == 44: 
                    try: 
                        gaap = re.findall('us-gaap_', rows[i][1])
                        dei  = re.findall('dei_', rows[i][1])
                        if gaap or dei: 
                            vals = [y[k] for k in body_set]
                            dic  = dict(zip(body_keys, vals))
                            if 'TextBlock' in dic['elementId']: 
                                pass
                            elif 'Axis' in dic['elementId']: 
                                pass
                            else: 
                                try: 
                                    p = int(y[0])
                                    entry['tags'][dic['elementId']] = dic
                                except: 
                                    pass
                    except: 
                        pass
                        # --
            entry['cik']            = entry['tags']['dei_EntityCentralIndexKey']['fact']
            #entry['ticker']         = entry['tags']['dei_TradingSymbol']
            entry['doc_type']       = entry['tags']['dei_DocumentType']['fact']
            entry['period_end']     = entry['tags']['dei_DocumentPeriodEndDate']['fact']
            #entry['filer_status']   = entry['tags']['dei_EntityFilerCategory']
            if entry['tags']['dei_DocumentType']['fact'] in ('10-K', '10-Q'):
                try: 
                    client.index(index = 'ernest_xbrl_raw', doc_type = 'filing', body = entry, id = doc)
                    print(doc)
                except: 
                    print('parsing exception')
            else: 
                print(entry['tags']['dei_DocumentType']['fact'])
        except csv.Error, e: 
            print(e)




# -- 
# run 

__ingest('2016', '01') 
