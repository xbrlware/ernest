#!/usr/bin/env python

'''
    Merges OTC halts with SEC trading suspensions

    ** Note **
    This runs prospectively after new OTC suspensions and SEC suspensions
    have been scraped
'''

import argparse
import json
import logging

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from fuzzywuzzy import fuzz


class MERGE_HALTS:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + '.merge_halts')

        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']}],
            timeout=60000)

        self.TARGET_INDEX = config['suspension']['index']
        self.TARGET_TYPE = config['suspension']['_type']
        self.REF_INDEX = config['otc_halts']['index']
        self.REF_TYPE = config['otc_halts']['_type']

    def get_max_date(self):
        query = {
            "size": 0,
            "aggs": {"max": {"max": {"field": "date"}}}
        }
        d = self.client.search(index=self.TARGET_INDEX, body=query)
        x = int(d['aggregations']['max']['value'])
        max_date = datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%d')
        return max_date

    def build_out(self, ratio, score, hits, body, a, hit):
        if ratio >= 65 and score >= 1 and hits > 0:
            out = {
                "_id": body["_id"],
                "_type": self.TARGET_TYPE,
                "_index": self.TARGET_INDEX,
                "_op_type": "update",
                "doc": {
                    "__meta__": {
                        'finra': {
                            'ticker': hit['SymbolName'],
                            'company': hit['CompanyName'],
                            'haltResumeID': hit['HaltResumeID'],
                            'haltID': hit['TradeHaltID'],
                            'haltReasonCode': hit['HaltReasonCode'],
                            'marketCat': hit['MarketCategoryLookup'],
                            'dateHalted': hit['_enrich']['halt_long_date'],
                            'dateLoaded': hit['_enrich']['load_long_date'],
                            'score': body['_score'],
                            'ratio': ratio,
                            'secHalt': True,
                            'matched': True
                        }
                    }
                }
            }
        else:
            out = {
                "_id": a['_id'],
                "_type": self.TARGET_TYPE,
                "_index": self.TARGET_INDEX,
                "_op_type": "index",
                "_source": {
                    "date": hit['_enrich']['halt_short_date'],
                    "company": hit['SymbolName'],
                    "link": None,
                    "release_number": None,
                    "__meta__": {
                        'finra': {
                            'ticker': hit['SymbolName'],
                            'company': hit['CompanyName'],
                            'haltResumeID': hit['HaltResumeID'],
                            'haltID': hit['TradeHaltID'],
                            'haltReasonCode': hit['HaltReasonCode'],
                            'marketCat': hit['MarketCategoryLookup'],
                            'dateHalted': hit['_enrich']['halt_long_date'],
                            'dateLoaded': hit['_enrich']['load_long_date'],
                            'score': 0,
                            'ratio': ratio,
                            'secHalt': True,
                            'matched': False
                        }
                    }
                }
            }
            return out

    def run(self, query):
        for a in scan(self.client, index=self.REF_INDEX, query=query):
            h = a['_source']
            if h['IsSECRelatedHalt'] == "Yes" and h['ActionDescription'] == 'Halt':
                res = self.client.search(index=self.TARGET_INDEX, body={
                    "sort": [
                        {
                            "_score": {
                                "order": "desc"
                            }
                        }],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "company": h['CompanyName']
                                    }
                                },
                                {
                                    "match": {
                                        "date": h['_enrich']['halt_short_date']
                                    }
                                }
                            ]
                        }
                    }
                })
                if res['hits']['total'] > 0:
                    mtc = res['hits']['hits'][0]['_source']
                    sym_name = h['CompanyName'].lower().replace(
                        'common stock', '').replace('ordinary shares', '')
                    halt_name = mtc['company'].lower()
                    x = fuzz.token_sort_ratio(sym_name, halt_name)
                    body = res['hits']['hits'][0]
                    out = self.build_out(x, body['_score'], 1, body, a, h)
                    out_log = {
                        "_id": a['_id'],
                        "_type": self.REF_TYPE,
                        "_index": self.REF_INDEX,
                        "_op_type": "update",
                        "doc": {
                            "__meta__": {
                                "match_attempted": True,
                                "match_success": True
                            }
                        }
                    }
                elif res['hits']['total'] == 0:
                    out = self.build_out(0, 0, 0, 0, a, h)
                    out_log = {
                        "_id": a['_id'],
                        "_type": self.REF_TYPE,
                        "_index": self.REF_INDEX,
                        "_op_type": "update",
                        "doc": {
                            "__meta__": {
                                "match_attempted": True,
                                "match_success": False
                            }
                        }
                    }

                if out:
                    yield out

                yield out_log

            else:
                pass

    def run2(self, query):
        for a in scan(self.client, index=self.REF_INDEX, query=query):
            h = a['_source']
            if h['IsSECRelatedHalt'] == "No" and h['ActionDescription'] == 'Halt':
                _id = a['_id']
                out = {
                    "_id": _id,
                    "_type": self.TARGET_TYPE,
                    "_index": self.TARGET_INDEX,
                    "_op_type": "index",
                    "_source": {
                        "date": h['_enrich']['halt_short_date'],
                        "company": h['SymbolName'],
                        "link": None,
                        "release_number": None,
                        "__meta__": {
                            'finra': {
                                'ticker': h['SymbolName'],
                                'company': h['CompanyName'],
                                'haltResumeID': h['HaltResumeID'],
                                'haltID': h['TradeHaltID'],
                                'haltReasonCode': h['HaltReasonCode'],
                                'marketCat': h['MarketCategoryLookup'],
                                'dateHalted': h['_enrich']['halt_long_date'],
                                'dateLoaded': h['_enrich']['load_long_date'],
                                'score': 0,
                                'ratio': 0,
                                'secHalt': False,
                                'matched': False
                            }
                        }
                    }
                }
                out_log = {
                    "_id": a['_id'],
                    "_type": self.REF_TYPE,
                    "_index": self.REF_INDEX,
                    "_op_type": "update",
                    "doc": {
                        "__meta__": {
                            "match_attempted": True,
                            "match_success": True
                        }
                    }
                }
                if out:
                    yield out
                yield out_log

            else:
                pass

    def main(self):
        if self.args.from_scratch:
            query = {
                "query": {
                    "range": {
                        "_enrich.halt_short_date": {
                            "lte": self.get_max_date()
                        }
                    }
                }
            }
        elif self.args.most_recent:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "_enrich.halt_short_date": {
                                        "lte": self.get_max_date()
                                    }
                                }
                            },
                            {
                                "filtered": {
                                    "filter": {
                                        "missing": {
                                            "field": "__meta__.match_attempted"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }

        for a, b in streaming_bulk(self.client,
                                   self.run(query),
                                   chunk_size=1000,
                                   raise_on_error=False):
            print(a, b)

        for a, b in streaming_bulk(self.client,
                                   self.run2(query),
                                   chunk_size=1000,
                                   raise_on_error=False):
            print(a, b)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", type=str,
                        action='store', default='../config.json')
    parser.add_argument("--most-recent", action='store_true')
    parser.add_argument("--from-scratch", action='store_true')
    args = parser.parse_args()

    mh = MERGE_HALTS(args)
    mh.main()
