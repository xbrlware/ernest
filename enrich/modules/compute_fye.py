#!/usr/bin/env python

import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from query_json.fye_queries import fye_from_scratch, fye_most_recent


class COMPUTE_FYE:
    def __init__(self, args, parent_logger):
        with open('/home/ubuntu/ernest/config.json', 'rb') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{'host': config['es']['host'],
                                      'port': config['es']['port']}
                                     ])
        self.logger = logging.getLogger(parent_logger + '.compute_fye')
        self.aq_index = config['aq_forms_enrich']['index']
        self.aq_doc = config['aq_forms_enrich']['_type']
        self.sb_index = config['sub_agg']['index']
        self.sb_doc = config['sub_agg']['_type']
        if args.from_scratch:
            self.query = fye_from_scratch()
        elif args.most_recent:
            self.query = fye_most_recent()

    def __build_schedule(self, id, x):
        mon_day = x['_enrich']['period'][5:].split('-')
        return {
            'id': id,
            'cik': x['cik'],
            'min_date': x['date'],
            'max_date': x['date'],
            'fYEMonth': mon_day[0],
            'fYEDay': mon_day[1]
            }

    def __get_id(self, x):
        return x['cik'] + '_' + x['_enrich']['period'][5:]

    def compute(self):
        for doc in scan(self.client,
                        index=self.aq_index,
                        doc_type=self.aq_doc,
                        query=self.query):
            src = doc['_source']
            id = self.__get_id(src)
            bs = self.__build_schedule(id, src)
            try:
                res = self.client.get(index=self.sb_index,
                                      doc_type=self.sb_doc,
                                      id=id)
                bs['min_date'] = res['_source']['min_date']
            except:
                self.logger.error(res)
            res = self.client.index(index=self.sb_index, doc_type=self.sb_doc,
                                    id=bs['id'], body=bs)
            self.logger.info(res)
