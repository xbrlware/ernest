#!/usr/bin/env python2.7

import json
import logging
import math
import re

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan, BulkIndexError

from generic.date_handler import DATE_HANDLER
from generic.key_checker import KEY_CHECKER

from http_handler import HTTP_HANDLER


class FINRA_DIRS:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger('scrape_finra.finra_dirs')
        self.dh = DATE_HANDLER('scrape_finra.finra_dirs')
        self.kc = KEY_CHECKER('scrape_finra.finra_dirs')
        self.browser = HTTP_HANDLER('scrape_finra.finra_dirs')

        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']}])
        ou = 'http://otce.finra.org'
        self.urls = {
            "directory": ou +
            '/Directories/DirectoriesJson?pgnum=',
            "halts": ou +
            '/TradeHaltsHistorical/TradeHaltsHistoricalJson?pgnum=',
            "delinquency": ou +
            '/DCList/DCListJson?pgnum='
        }
        self.INDEX = config['otc_%s' % args.directory]['index']
        self.TYPE = config['otc_%s' % args.directory]['_type']
        self.url = self.urls[args.directory]
        self.query = {
            "query": {
                "filtered": {
                    "filter": {
                        "missing": {
                            "field": "_enrich.updated_short_date"
                        }
                    }
                }
            }
        }

    def enrich_dates(self, body):
        try:
            lud = body['LastUpdatedDate']
        except:
            lud = None

        body['_enrich'] = {}
        body['_enrich']['updated_short_date'] = self.dh.ref_date(lud)
        body['_enrich']['updated_long_date'] = self.dh.long_date(lud)

        for key in body:
            if re.search('.*Date.*', key):
                body[key] = self.dh.ref_date(body[key])

        body['LastUpdatedDate'] = self.dh.ref_date(lud)
        return body

    def enrich(self):
        for doc in scan(self.client, index=self.INDEX, query=self.query):
            resp = self.client.update(
                index=self.INDEX,
                doc_type=self.TYPE,
                id=doc["_id"],
                body={"doc": self.enrich_dates(doc['_source'])}
            )
            if resp['_shards']['failed'] > 0:
                self.logger.debug(resp)

    def max_date(self, u_index):
        query = {
            "size": 0,
            "aggs": {"max": {"max": {"field": "_enrich.halt_short_date"}}}
        }
        d = self.client.search(index=u_index, body=query)
        try:
            x = int(d['aggregations']['max']['value'])
            max_date = datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%d')
        except TypeError:
            # this is raised if the query returned a null
            max_date = '1900-01-01'
        return max_date

    def handle_directory(self, build_update, dir_func):
        session = self.browser.create_session()
        x = self.browser.get_page(session, self.url + str(1), "json")
        r = x['iTotalRecords']
        n = int(math.ceil(float(r) / 25))
        for i in range(0, n + 1):
            x = self.browser.get_page(session, self.url + str(i), "json")
            out = x['aaData']
            es = dir_func(out)
            es_length = len(es)
        try:
            for a in parallel_bulk(self.client, es):
                pass
        except BulkIndexError as e:
            self.logger.info(
                "[BulkIndexError]|{0}|of {1} docs {2}".format(
                    e[1][0]['create']['status'],
                    es_length,
                    e[0]))
            for doc in e[1]:
                self.logger.info(doc['create']['error']['reason'])

    def build_directory(self, out):
        dir_docs = []
        for i in out:
            if self.args.directory == 'halts':
                _id = str(i['HaltResumeID']) + '_' + str(i['SecurityID'])
            elif self.args.directory == 'directory':
                _id = str(i['SecurityID']) + '_' + str(i['IssuerId'])
            elif self.args.directory == 'delinquency':
                _id = str(i['SecurityID']) + '_' + str(i['DCList_ID'])

            dir_docs.append({
                "_op_type": "create",
                "_index": self.INDEX,
                "_type": self.TYPE,
                "_id": _id,
                "_source": self.enrich_dates(i)
            })
        return dir_docs

    def update_directory(self, out):
        dir_docs = []
        if self.dh.ref_date(out[0]['DateHalted']) >= self.max_date(self.INDEX):
            for i in out:
                _id = str(i['HaltResumeID']) + '_' + str(i['SecurityID'])
                dir_docs.append({
                    "_op_type": "create",
                    "_index": self.INDEX,
                    "_type": self.TYPE,
                    "_id": _id,
                    "_source": self.enrich_dates(i)
                })
        return dir_docs

    def main(self):
        resp = self.client.count(index=self.INDEX)
        count_in = resp['count'] or None

        if self.args.update_halts:
            self.logger.info('Start update of finra directory')
            self.handle_directory('update', self.update_directory)
        else:
            self.logger.info('Start building of finra directory')
            self.handle_directory('build', self.build_directory)

        self.logger.info('checking enrich dates in otc index')
        self.enrich()

        resp = self.client.count(index=self.INDEX)
        count_out = resp['count'] or None
        self.logger.info('{0} in, {1} out'.format(count_in, count_out))
        return [count_in, count_out]
