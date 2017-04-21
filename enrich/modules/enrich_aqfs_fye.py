import datetime
import json
import logging
import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from query_json.aqfs_fye_queries import aqfs_fye_query
from query_json.aqfs_fye_queries import aqfs_fye_query_func
from query_json.aqfs_fye_queries import add_id_query
from query_json.aqfs_fye_queries import add_id_sub_query


class ENRICH_AQFS_FYE:
    def __init__(self, args, parent_logger):
        with open(args.config_path, 'rb') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']}
            ])
        self.args = args
        self.query = aqfs_fye_query()
        self.query_func = aqfs_fye_query_func()
        self.logger = logging.getLogger(parent_logger + '.enrich')

    def get_fye(self, period, fYEMonth, fYEDay):
        fyeTest = period[:5] + fYEMonth + '-' + fYEDay
        if self.to_datetime(period) > self.to_datetime(fyeTest):
            fye = str(int(period[:4]) + 1) + '-' + fYEMonth + '-' + fYEDay
        elif self.to_datetime(period) <= (self.to_datetime(fyeTest)):
            fye = fyeTest
        return fye

    def to_datetime(self, x):
        r = map(int, x.split('-'))
        return datetime.date(r[0], r[1], r[2])

    def add_id(self, doc):
        doc['sub'] = {}
        query = add_id_query(doc['cik'], doc['_enrich']['period'])
        hit = []
        for a in scan(self.client,
                      index=self.config['sub_agg']['index'],
                      query=query):
            hit.append(a)
        if len(hit) == 1:
            ref = hit[0]['_source']
            period = doc['_enrich']['period']
            fye = self.get_fye(period, ref['fYEMonth'], ref['fYEDay'])
            fp = self.get_fp(fye, period)
            doc['sub']['fiscalYearEnd'] = fye
            doc['sub']['fiscalPeriod'] = fp
            doc['sub']['matched'] = 'function'
            try:
                doc['sub']['fpID'] = fp + '__' + fye[:4]
            except:
                doc['sub']['fpID'] = None
        else:
            doc['sub']['fiscalYearEnd'] = None
        return doc

    def get_fp(self, fye, period):
        delta = self.to_datetime(fye) - self.to_datetime(period)
        p = round(float(delta.days) / float(30))
        if p > 7:
            qtr = 'Q1'
        elif p > 4:
            qtr = 'Q2'
        elif p > 1:
            qtr = 'Q3'
        elif p == 0:
            qtr = 'Q4'
        else:
            qtr = None
        return qtr

    def parse_adsh(self, body):
        acc = re.compile("\d{5,}-\d{2}-\d{3,}")
        val = re.findall(acc, body['url'])[0]
        return val

    def add_id_sub(self, doc):
        default = doc
        fye = doc.get('sub', None)
        if fye is not None:
            fye = doc.get('fiscalYearEnd', None)
        doc['sub'] = {}
        acc = self.parse_adsh(doc)
        query = add_id_sub_query(acc)
        hit = []
        for a in scan(self.client,
                      index=self.config['xbrl_submissions']['index'],
                      query=query):
            hit.append(a)
        if len(hit) == 1:
            ref = hit[0]['_source']
            fp = ref['fp']
            fy = ref['fy']
            if fp == "FY":
                fp = "Q4"
            doc['sub']['fiscalYearEnd'] = fye
            doc['sub']['fiscalPeriod'] = fp
            doc['sub']['matched'] = 'acc_no'
            try:
                doc['sub']['fpID'] = fp + '__' + fy
            except:
                doc['sub']['fpID'] is None
            return doc
        else:
            return default

    def es_index(self, q, id_func):
        for a in scan(self.client,
                      index=self.config['aq_forms_enrich']['index'],
                      query=q):
            try:
                res = self.client.index(
                    index=self.config['aq_forms_enrich']['index'],
                    doc_type=self.config['aq_forms_enrich']['_type'],
                    id=a["_id"],
                    body=id_func(a['_source'])
                )
                self.logger.info(res)
            except:
                self.logger.error('{0}|{1}'.format(a['_id'], a))

    def enrich(self):
        if self.args.function:
            self.es_index(self.query, self.add_id)

        if self.args.sub:
            self.es_index(self.query_func, self.add_id_sub)
