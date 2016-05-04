import csv, os, json, re
import argparse
import itertools
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

from datetime import datetime
from datetime import date, timedelta


# --
# es connection

client = Elasticsearch([{
    'host' : 'localhost', #config["es"]["host"], 
    'port' : 9205 #config["es"]["port"]
}], timeout = 60000)


# --
# functions

# - 
def __parse_sheets(out, sc): 
    split    = sc.split('&&')
    _sheet   = split[0]
    _context = split[1]
    sheet   = [j for j in out if _sheet in j[1] and _context in j[6]]
    top_level = list(set([j[2] for j in sheet]) - set([j[3] for j in sheet]))
    tree      = {} 
    for k in top_level: 
        tree[k] = {} 
        sub     = [x[3] for x in sheet if x[2] == k]
        for s in sub: 
            z = [x[3] for x in sheet if x[2] == s]
            if len(z) == 0: 
                fact    = [p[8] for p in sheet if p[3] == s]
                decimal = [p[9] for p in sheet if p[3] == s]
                balance = [p[10] for p in sheet if p[3] == s]
                string  = [p[12] for p in sheet if p[3] == s]
                # __ should add validation for lengths here
                tree[k][s] = {
                    'fact'    : fact[0],
                    'decimal' : decimal[0],
                    'balance' : balance[0], 
                    'string'  : string[0]
                } 
            elif len(z) != 0: 
                tree[k][s] = {} 
                for m in z: 
                    fact    = [p[8] for p in sheet if p[3] == m]
                    decimal = [p[9] for p in sheet if p[3] == m]
                    balance = [p[10] for p in sheet if p[3] == m]
                    string  = [p[12] for p in sheet if p[3] == m]
                    # __ should have length validation here too
                    tree[k][s][m] = {
                    'fact'    : fact[0],
                    'decimal' : decimal[0],
                    'balance' : balance[0], 
                    'string'  : string[0]
                    } 
    return tree


# - 
def __dei_tree(dei_frame): 
    k = dei_frame
    k.sort()
    c = list(k for k,_ in itertools.groupby(k))
    dei_tree = {} 
    for i in c: 
        dei_tree[i[0]] = i[2]
    return dei_tree


# -
def __build_object(tag_frame): 
    out = []
    for c in range(0, len(tag_frame)): 
        x = tag_frame[c]
        if 'Axis' in x[0] or 'TextBlock' in x[0] or 'Axis' in x[6]: 
            pass
        else: 
            try: 
                if int(x[5]): 
                    if '/role/label' in x[11]: 
                        out.append(x)
                    else: 
                        pass
                else: 
                    pass
            except ValueError: 
                pass
                # ---
    return out


# - 
def __ingest(year, month): 
    path   = '/home/ubuntu/xbrl/' + year + '/' + month + '/parsed'
    for x in os.listdir(path):
        try: 
            doc    = path + '/' + x
            # doc    = '/Users/culhane/Desktop/xbrl_test/test_docs/test_join_2.csv'
            f      = open(doc, 'rU') 
            reader = csv.reader(f)
            rows   = list(reader)
            entry  = {
                "link" : x,
                "year" : year,
                "month": month,
                "statements"   : {}, 
                "entity_info"  : {}
            }
            # --- define list inputs
            indices   = [1, 2, 3, 4, 6, 13, 14, 15, 16, 17, 27, 30, 31]
            frame     = []
            for i in range(1, len(rows)): 
                try: 
                    row = [rows[i][k] for k in indices]
                    frame.append(row)
                except: 
                    pass
                    # ---
            dei_frame = [[frame[i][0], frame[i][6], frame[i][8]] for i in range(0, len(frame)) if 'dei_' in frame[i][0]]
            tag_frame = [frame[i] for i in range(0, len(frame)) if 'us-gaap_' in frame[i][0]] 
            # --- structure doc entity information
            entry['entity_info'] = __dei_tree(dei_frame)
            # --- reduce and structure
            if entry['entity_info']['dei_DocumentType'] in ('10-K', '10-Q'):
                out        = __build_object(tag_frame)
                __sections = list(set([i[1] + '&&' + i[6] for i in out]))
                sections   = []
                for i in __sections: 
                    x = re.findall('/role/.*&&.*', i)[0]
                    sections.append(x.replace('/role/', ''))
                    # ---
                for sc in sections: 
                    c = sc.split('&&')
                    try:
                        p = entry['statements'][c[0]]
                        entry['statements'][c[0]][c[1]] = __parse_sheets(out, sc)
                    except: 
                        entry['statements'][c[0]]       = {} 
                        entry['statements'][c[0]][c[1]] = __parse_sheets(out, sc)
                # --- index 
                try: 
                    client.index(index = 'ernest_xbrl', doc_type = 'filing', body = entry, id = entry['link'])
                except: 
                    print(' -- parsing exception -- ')
            else: 
                print(entry['entity_info']['dei_DocumentType'])
        except csv.Error, e: 
            print(e)

# -- 
# run 

__ingest('2016', '01') 
