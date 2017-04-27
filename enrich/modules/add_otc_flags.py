#!/usr/bin/env python

'''
    Adds is_otc flag to documents in ownership and symbology indices

    ** Note **
    This runs prospectively each day after the edgar and otc ndex scrapes
'''

import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk


class ADD_OTC_FLAGS:
    def __init__(self, args):
        self.logger = logging.getLogger('enrich_add_otc_flag.add_otc_flags')
        self.args = args
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']
        }], timeout=60000)

    def main(self):
        for a, b in streaming_bulk(self.client, self.gen(), chunk_size=2500):
            if a is not True:
                self.logger.debug(b)

    def gen(self):
        look = self.client.search(
            index=self.config['otc_directory']['index'],
            body={
                "size": 0,
                "aggs": {
                    "terms": {
                        "terms": {
                            "field": "SymbolName",
                            "size": 1000000
                        }
                    }
                }
            })['aggregations']['terms']['buckets']

        lookup = set([l['key'].upper() for l in look])

        query = {
            "_source": self.args.field_name,
            "query": {
                "filtered": {
                    "filter": {
                        "and": [
                            {
                                "missing": {
                                    "field": "__meta__.is_otc"
                                }
                            },
                            {
                                "exists": {
                                    "field": self.args.field_name
                                }
                            }
                        ]
                    }
                }
            }
        }

        for doc in scan(self.client,
                        index=self.config[self.args.index]['index'],
                        query=query):
            try:
                yield {
                    "_index": doc['_index'],
                    "_type": doc['_type'],
                    "_id": doc['_id'],
                    "_op_type": "update",
                    "doc": {
                        "__meta__": {
                            "is_otc": doc['_source']
                            [self.args.field_name].upper() in lookup
                        }
                    }
                }
            except:
                # This happens if the field_name is missing
                self.logger.debug('[field_name missing] {}'.format(doc))
                pass
