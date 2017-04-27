#!/usr/bin/env python
'''
    Matches names to CIK numbers via Elasticsearch query
'''

import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan


class TO_CIK:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + '.to_cik')
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']
        }], timeout=60000)

    def run_name_to_cik(self):
        query = {
            "_source": self.args.name_to_cik_field_name,
            "query": {
                "filtered": {
                    "filter": {
                        "and": [
                            {
                                "missing": {
                                    "field": "__meta__.sym.match_attempted"
                                }
                            },
                            {
                                "exists": {
                                    "field": self.args.name_to_cik_field_name
                                }
                            }
                        ]
                    }
                }
            }
        }
        total_count = self.client.count(
            index=self.config[self.args.index]['index'],
            body=query)['count']

        counter = 0
        for a in scan(self.client,
                      index=self.config[self.args.index]['index'], query=query):
            sym = {"match_attempted": True}
            res = self.client.search(
                index=self.config['symbology']['index'],
                body={
                    "size": 1,
                    "_source": "cik",
                    "query": {
                        "match_phrase": {
                            "name": a['_source']
                            [self.args.name_to_cik_field_name]
                        }
                    }
                })['hits']['hits']

            if res:
                if res[0]['_score'] > self.args.threshold:
                    sym.update(res[0]['_source'])

            yield {
                "_index": a['_index'],
                "_type": a['_type'],
                "_id": a['_id'],
                "_op_type": "update",
                "doc": {
                    "__meta__": {
                        "sym": sym
                    }
                }
            }
            counter += 1
        self.logger.info('[COMPLETED]|{0} out of {1}'.format(counter,
                                                             total_count))

    def get_lookup(self):
        query = {
            "_source": ["max_date", "sic", "cik", "ticker", "name"],
            "query": {
                "filtered": {
                    "filter": {
                        "exists": {
                            "field": "ticker"
                        }
                    }
                }
            }
        }

        out = {}
        for a in scan(self.client,
                      index=self.config['symbology']['index'], query=query):
            out[a['_source']['ticker']] = a['_source']

        return out

    def run_ticker_to_cik(self):
        field_name = self.args.ticker_to_cik_field_name
        self.logger.info('[{}]|ticker to cik'.format(field_name))
        if not self.args.halts:
            query = {
                "fields": field_name,
                "query": {
                    "filtered": {
                        "filter": {
                            "and": [
                                {
                                    "missing": {
                                        "field": "__meta__.sym.match_attempted"
                                    }
                                },
                                {
                                    "exists": {
                                        "field": field_name
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        elif self.args.halts:
            query = {
                "fields": field_name,
                "query": {
                    "filtered": {
                        "filter": {
                            "and": [
                                {
                                    "missing": {
                                        "field": "__meta__.sym.cik"
                                    }
                                },
                                {
                                    "exists": {
                                        "field": field_name
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        total_count = self.client.count(
            index=self.config[self.args.index]['index'],
            body=query)['count']

        counter = 0
        for a in scan(self.client,
                      index=self.config[self.args.index]['index'], query=query):
            sym = {"match_attempted": True}
            gl = self.get_lookup()
            mtc = gl.get(a['fields'][self.args.ticker_to_cik_field_name][0], {})
            sym.update(mtc)
            yield {
                "_id": a['_id'],
                "_type": a['_type'],
                "_index": a['_index'],
                "_op_type": "update",
                "doc": {
                    "__meta__": {
                        "sym": sym
                    }
                }
            }
            counter += 1
        self.logger.info(
            '[COMPLETED]|{0} out of {1}'.format(counter, total_count))

    def name_to_cik(self):
        for a, b in streaming_bulk(self.client,
                                   self.run_name_to_cik(),
                                   chunk_size=100):
            self.logger.info("[NAMETOCIK]|{0},{1}".format(a, b))

    def ticker_to_cik(self):
        for a, b in streaming_bulk(self.client,
                                   self.run_ticker_to_cik(),
                                   chunk_size=1000):
            self.logger.info("[TICKERTOCIK]|{0},{1}".format(a, b))

    def main(self):
        self.name_to_cik()
        self.ticker_to_cik()
