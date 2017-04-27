#!/usr/bin/env python

'''
    Add single neighbor tag for owners and issuers to ownership index;
    tag enables hiding terminal nodes in front end

    ** Note **
    This runs prospectively using the --most-recent argument
'''

import argparse
import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan


class ENRICH_TERMINAL_NODES:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + ".terminal_nodes")
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']}
        ])
        self.match_all = {
            "query": {
                "match_all": {}
            }
        }

    def raw_dict(self, x, dict_type):
        if dict_type == 'issuer':
            key = x['issuerCik']
            val = x['ownerCik']
        elif dict_type == 'owner':
            key = x['ownerCik']
            val = x['issuerCik']
        return {
            "key": key,
            "value": val
        }

    def build_query(self, val):
        val = '__meta__.' + val + '_has_one_neighbor'
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "filtered": {
                                "filter": {
                                    "missing": {
                                        "field": val
                                    }
                                }
                            }
                        },
                        {
                            "match": {
                                val: True
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        return query

    def get_terminal_nodes(self, search_type):
        temp_dict = {}
        for a in scan(self.client,
                      index=self.config['ownership']['index'],
                      query=self.match_all):
            x = self.raw_dict(a['_source'], search_type)
            if x["key"] in temp_dict:
                if temp_dict[x["key"]]["terminal"] is True:
                    if x["value"] != temp_dict[x["key"]]["value"]:
                        temp_dict[x["key"]]["terminal"] = False
                    else:
                        pass
                else:
                    pass
            else:
                temp_dict[x["key"]] = {
                    "value": x["value"],
                    "terminal": True
                }

        return [key for key in temp_dict if temp_dict[key]['terminal'] is True]

    def get_update_nodes(self, query_type):
        gtn = self.get_terminal_nodes(query_type)

        if self.args.from_scratch:
            query = self.build_query(query_type)
        else:
            query = {"query": {
                "bool": {
                    "must_not": {
                        "match": {
                            "__meta__." + query_type + "_has_one_neighbor": True
                        }
                    },
                    "must": {
                        "terms": {
                        }
                    }
                }
            }}

        return query, gtn

    def main(self, query_type):
        actions = []
        query, t_nodes = self.get_update_nodes(query_type)
        i = 0

        tn = [t_nodes[j: j + 1024] for j in range(0, len(t_nodes), 1024)]
        for p in tn:
            query["query"]["bool"]["must"]["terms"][query_type + "Cik"] = p
            for person in scan(self.client,
                               index=self.config['ownership']['index'],
                               query=query):
                actions.append({
                    "_op_type": "update",
                    "_index": self.config['ownership']['index'],
                    "_id": person['_id'],
                    "_type": person['_type'],
                    "doc": {
                        "__meta__": {
                            query_type + "_has_one_neighbor": True
                        }
                    }
                })
                i += 1

                if i > 500:
                    for success, info in parallel_bulk(self.client,
                                                       actions,
                                                       chunk_size=510):
                        if not success:
                            self.logger.error('[RESPONSE]|{}'.format(info))
                        else:
                            self.logger.info('[RESPONSE]|{}'.format(info))

                    actions = []
                    i = 0

        for success, info in parallel_bulk(self.client,
                                           actions,
                                           chunk_size=510):
            if not success:
                self.logger.error('[RESPONSE]|{}'.format(info))
            else:
                self.logger.info('[RESPONSE]|{}'.format(info))

        f_query = {
            "query": {
                "bool": {
                    "must_not": {
                        "terms": {
                            query_type + 'Cik': t_nodes
                        }
                    },
                    "must": {
                        "match": {
                            "__meta__." + query_type + "_has_one_neighbor": True
                        }
                    }
                }
            }
        }

        for a in scan(self.client,
                      index=self.config['ownership']['index'],
                      query=f_query):
            a['_source']['__meta__'][query_type + '_has_one_neighbor'] = False
            res = self.client.index(
                index=self.config['ownership']['index'],
                doc_type=a['_type'],
                body=a['_source'],
                id=a['_id']
            )
            self.logger.info(res)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='add single neighbor tags')
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--most-recent',
                        dest='most_recent',
                        action="store_true")
    parser.add_argument('--config-path',
                        type=str,
                        action='store',
                        default='../config.json')

    args = parser.parse_args()
