import holidays
import json
import logging

from datetime import timedelta, date

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from elasticsearch.helpers import parallel_bulk

from misc_json.download_json import download_json
from query_json.compute_delinquency_query import compute_delinquency_query


class COMPUTE_DELINQUENCY:
    def __init__(self, args, parent_logger):
        with open(args.config_path, 'rb') as inf:
            config = json.load(inf)
            self.config = config
        self.logger = logging.getLogger(parent_logger + '.compute_delinquency')
        self.client = Elasticsearch([{'host': config['es']['host'],
                                      'port': config['es']['port']
                                      }], timeout=60000)
        self.dl = download_json()
        self.query = compute_delinquency_query()

    def __pre_post(self, enrich_period, period, status, pre, post):
        if enrich_period < period:
            return self.dl[status][pre]
        else:
            return self.dl[status][post]

    def __accelerated_filer(self, src):
        if src['form'] == "10-K":
            period = "2003-12-15"
            pre = "preDec1503"
            post = "postDec1503"
        elif src['form'] == "10-Q":
            period = "2004-12-15"
            pre = "preDec1504"
            post = "postDec1504"
        return self.__pre_post(src['_enrich']['period'], period,
                               src['_enrich']['status'], pre, post)

    def __large_accelerated_filer(self, src):
        if src['form'] == "10-K":
            return self.__pre_post(src['_enrich']['period'],
                                   '2006-12-15', src['_enrich']['status'],
                                   'preDec1506', 'postDec1506')
        elif src['form'] == "10-Q":
            return self.dl[src['form']][src['_enrich']['status']]

    def __get_days(self, src):
        if src['_enrich']['status'] == "Large Accelerated Filer":
            ndays = self.__large_accelerated_filer(src)
        elif src['_enrich']['status'] == "Accelerated Filer":
            ndays = self.__accelerated_filer(src)
        else:
            ndays = self.dl[src['form']][src['_enrich']['status']]

        return ndays

    def __add_delinquency(self, src, us_holidays=holidays.US()):
        r = map(int, src['_enrich']['period'].split('-'))
        d = date(r[0], r[1], r[2])
        ndays = self.__get_days(src)
        dl = d + timedelta(days=ndays)
        while (dl in us_holidays) or (dl.weekday() >= 5):
            dl += timedelta(days=1)

        filed = map(int, src['date'].split('-'))
        filed = date(filed[0], filed[1], filed[2])
        src['_enrich']['deadline'] = dl.strftime("%Y-%m-%d")
        src['_enrich']['days_to_deadline'] = (dl - filed).days
        src['_enrich']['is_late'] = src['_enrich']['days_to_deadline'] < 0

        return src

    def __es_index(self, actions):
        for success, info in parallel_bulk(self.client, actions):
            if success:
                self.logger.info(info)
            else:
                self.logger.error(info)
        return []

    def compute_delinquency(self):
        i = 0
        actions = []
        for doc in scan(self.client,
                        index=self.config['aq_forms_enrich']['index'],
                        query=self.query):
            actions.append({
                '_op_type': 'index',
                '_index': self.config['aq_forms_enrich']['index'],
                '_type': self.config['aq_forms_enrich']['_type'],
                '_id': doc["_id"],
                '_source': self.__add_delinquency(doc['_source'])
                })
            i += 1
            if i > 500:
                actions = self.__es_index(actions)
        actions = self.__es_index(actions)
