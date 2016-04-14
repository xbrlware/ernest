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
                "statements"   : {} 
                "entity_info"  : {}
            }
            # --- define list inputs
            indices   = [1, 2, 3, 4, 6, 13, 14, 15, 16, 17, 27, 30, 31]
            frame     = [[rows[x][i] for i in indices] for x in range(1, len(rows))]
            dei_frame = [[frame[i][0], frame[i][6], frame[i][8]] for i in range(0, len(frame)) if 'dei_' in frame[i][0]]
            tag_frame = [frame[i] for i in range(0, len(frame)) if 'us-gaap_' in frame[i][0]] 
            # --- structure doc entity information
            entry['entity_info'] = __dei_tree(dei_frame)
            # --- reduce and structure
            if entry['entity_info']['dei_DocumentType'] in ('10-K', '10-Q'):
                context              = __reduce_context(dei_frame, tag_frame)
                out                  = __build_object(tag_frame, context)
                # --- structure balance sheet information 
                entry['statements']['balance_sheet'] = __balance_sheet(out)
                # --- index 
                try: 
                    client.index(index = 'ernest_xbrl', doc_type = 'filing', body = entry, id = x)
                    print(x)
                except: 
                    print(' -- parsing exception -- ')
            else: 
                print(entry['tags']['dei_DocumentType']['fact'])




def __dei_tree(dei_frame): 
    k = dei_frame
    k.sort()
    c = list(k for k,_ in itertools.groupby(k))
    dei_tree = {} 
    for i in c: 
        dei_tree[i[0]] = i[2]
    return dei_tree





def __balance_sheet(out): 
    b_sheet   = [i for i in out if 'BalanceSheet' in i[1]]
    top_level = list(set([i[2] for i in b_sheet]) - set([i[3] for i in b_sheet]))
    tree      = {} 
    for i in top_level: 
        tree[i] = {} 
        sub     = [x[3] for x in b_sheet if x[2] == i]
        for s in sub: 
            z = [x[3] for x in b_sheet if x[2] == s]
            if len(z) == 0: 
                fact    = [p[8] for p in b_sheet if p[3] == s]
                decimal = [p[9] for p in b_sheet if p[3] == s]
                balance = [p[10] for p in b_sheet if p[3] == s]
                string  = [p[12] for p in b_sheet if p[3] == s]
                # __ should add validation for lengths here
                tree[i][s] = {
                    'fact'    : fact[0],
                    'decimal' : decimal[0],
                    'balance' : balance[0], 
                    'string'  : string[0]
                } 
            elif len(z) != 0: 
                tree[i][s] = {} 
                for m in z: 
                    fact    = [p[8] for p in b_sheet if p[3] == m]
                    decimal = [p[9] for p in b_sheet if p[3] == m]
                    balance = [p[10] for p in b_sheet if p[3] == m]
                    string  = [p[12] for p in b_sheet if p[3] == m]
                    # __ should have length validation here too
                    tree[i][s][m] = {
                    'fact'    : fact[0],
                    'decimal' : decimal[0],
                    'balance' : balance[0], 
                    'string'  : string[0]
                    } 
    return tree





def __build_object(tag_frame, context): 
    out = []
    for c in range(0, len(tag_frame)): 
        x = tag_frame[c]
        if 'Axis' in x[0] or 'TextBlock' in x[0] or 'Axis' in x[6]: 
            pass
        else: 
            try: 
                if int(x[5]): 
                    if x[6] in context: 
                        if '/role/label' in x[11]: 
                            out.append(x)
                        else: 
                            pass
                    else: 
                        pass
                else: 
                    pass
            except ValueError: 
                pass
                # ---
    return out





def __reduce_context(dei_frame, tag_frame): 
    context   = []
    end_date  = [dei_frame[i] for i in range(0, len(dei_frame)) if dei_frame[i][0] == 'dei_DocumentPeriodEndDate']
    doc_type  = [dei_frame[i] for i in range(0, len(dei_frame)) if dei_frame[i][0] == 'dei_DocumentType']
    d         = list(set([end_date[i][2] for i in range(0, len(end_date))]))
    t         = list(set([doc_type[i][2] for i in range(0, len(end_date))]))
    # --- check for distinct / legible report period
    if len(d) == 1: 
        c_id   = d[0].replace('-', '')
    else: 
        print('-- invalid or missing context information --')
        # --- 
    contexts  = list(set([i[6] for i in tag_frame if 'Axis' not in i[6] and c_id in i[6]]))
    for i in contexts: 
        x = re.findall('_20\d{6}', i)
        if len(x) == 1: 
            context.append(i)
        elif len(x) == 2: 
            dates = [m.replace('_', '') for m in x]
            dates = [datetime(year=int(s[0:4]), \
                             month=int(s[4:6]), \
                             day=int(s[6:8])) for s in dates]
            dif   = (dates[1] - dates[0]).days
            # --- reduce by document type
            if t[0] == '10-Q':
                if dif > 100: 
                    print('-- c_id outside context window --')
                elif dif < 100: 
                    context.append(i)
            elif t[0] == '10-K':
                if dif > 380 or dif < 340: 
                    print('-- c_id outside context window --')
                elif dif < 380 and dif > 340: 
                    context.append(i)
            else: 
                print('-- c_id outside context window --')
        else: 
            print('--too many date values to parse --')
            # ---
    return context



# -- 
# run 

__ingest('2016', '01') 



