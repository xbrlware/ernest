#!/usr/bin/env python

'''
    Add sic text descriptions to symbology and ownership aggregation indices

    ** Note **
    This runs prospectively each day after the edgar filings scrape has been run
'''

import argparse
import json
import logging
import pickle

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk


class ADD_SIC_DESCRIPTION:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + '.add_sic_description')
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']
        }], timeout=60000)

        with open('../reference/sic_ref.p', 'rb') as pf:
            self.lookup = pickle.load(pf)
        self.query = {
            "_source": "sic",
            "query": {
                "filtered": {
                    "filter": {
                        "and": [
                            {
                                "missing": {
                                    "field": "__meta__.sic_lab"
                                }
                            },
                            {
                                "exists": {
                                    "field": "sic"
                                }
                            }
                        ]
                    }
                }
            }
        }

    def main(self, index_name):
        for a, b in streaming_bulk(self.client,
                                   self.gen(index_name),
                                   chunk_size=2500):
            pass

    def gen(self, index_name):
        total_count = self.client.count(index=self.config[index_name]['index'],
                                        body=self.query)['count']
        counter = 0
        for doc in scan(self.client,
                        index=self.config[index_name]['index'],
                        query=self.query):
            try:
                yield {
                    "_index": doc['_index'],
                    "_type": doc['_type'],
                    "_id": doc['_id'],
                    "_op_type": "update",
                    "doc": {
                        "__meta__": {
                            "sic_lab": self.lookup.get(
                                doc['_source']['sic'], None)
                        }
                    }
                }
                counter += 1
            except:
                self.logger.debug('[EXCEPTION]|{}'.format(doc))
                pass

        self.logger.info('[{0}]|Enriched {1} of {2} documents'.format(
            index_name.upper(), counter, total_count))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    args = parser.parse_args()
