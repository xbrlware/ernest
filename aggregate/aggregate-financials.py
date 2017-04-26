'''
    Aggregate financials information
'''

import json
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk


parser = argparse.ArgumentParser(description='financials')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch(host='localhost', port=9205)

# Update to use "has_financials" field
query = {
    "_source": [
        "cik",
        "date",
        "form",
        "__meta__.financials"
    ],
    "query": {
        "filtered": {
            "filter": {
                "exists": {
                    "field": "__meta__.financials"
                    }
                }
            }
        }
    }


def extract(docs):
    #  If there are other non-value fields here, add them or get errors
    fd = []
    for x in docs:
        for k, v in x['__meta__']['financials'].iteritems():
            if v and (k != 'interpolated'):
                fd.append({
                        "form": x['form'],
                        "date": x['date'],
                        "field": k,
                        "value": v['value']
                        })
    return fd


def build_docs():
    docs = {}
    for a in scan(client,
                  index=config['financials']['index'],
                  doc_type=config['financials']['_type'],
                  query=query):
        try:
            docs[a['_source']['cik']].append(a['_source'])
        except:
            try:
                docs[a['_source']['cik']] = [a['_source']]
            except KeyError as e:
                print(e)
    return docs


def stream():
    docs = build_docs()
    for key in docs:
        efd = extract(docs[key])
        fd = {
            "cik": key.zfill(10),
            "financials": efd,
            "financials_stringified": map(json.dumps, efd)
            }
        yield {
            "_op_type": "update",
            "_index": config['agg']['index'],
            "_type": config['agg']['_type'],
            "_id": key.zfill(10),
            "doc": fd,
            "doc_as_upsert": True
            }


for a, b in streaming_bulk(client, actions=[s for s in stream()]):
    print('{0}|{1}'.format(a, b))
