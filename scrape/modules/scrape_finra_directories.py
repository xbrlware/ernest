#!/usr/bin/env python2.7

import re
import json
import logging
import math
import urllib2

from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan


class FINRA_DIRS:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger('scrape_finra.finra_dirs')
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
        body['_enrich'] = {}
        body['_enrich']['updated_short_date'] = self.ref_date(
            body['LastUpdatedDate'])
        body['_enrich']['updated_long_date'] = self.long_date(
            body['LastUpdatedDate'])
        return body

    def ref_date(self, date):
        d = int(re.sub('\D', '', date))
        out_date = datetime.utcfromtimestamp(d / 1000).strftime('%Y-%m-%d')
        return out_date

    def long_date(self, date):
        d = int(re.sub('\D', '', date))
        out_date = datetime.utcfromtimestamp(d / 1000).strftime(
            '%Y-%m-%d %H:%M:%S')
        return out_date

    def enrich(self):
        for doc in scan(self.client, index=self.INDEX, query=self.query):
            resp = self.client.index(
                index=self.INDEX,
                doc_type=self.TYPE,
                id=doc["_id"],
                body=self.enrich_dates(doc['_source'])
            )
            self.logger.info(resp)

    def max_date(self, u_index):
        query = {
            "size": 0,
            "aggs": {"max": {"max": {"field": "_enrich.halt_short_date"}}}
        }
        d = self.client.search(index=u_index, body=query)
        x = int(d['aggregations']['max']['value'])
        max_date = datetime.utcfromtimestamp(x / 1000).strftime('%Y-%m-%d')
        return max_date

    def handle_directory(self, build_update, dir_func):
        x = json.load(urllib2.urlopen(self.url + str(1)))
        r = x['iTotalRecords']
        n = int(math.ceil(float(r) / 25))
        for i in range(0, n + 1):
            x = json.load(urllib2.urlopen(self.url + str(i)))
            out = x['aaData']
            es = dir_func(out)
        for a, b in parallel_bulk(self.client, es):
            if a is True:
                self.logger.info(b)
            else:
                self.logger.debug(b)

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
                "_op_type": "index",
                "_index": self.INDEX,
                "_type": self.TYPE,
                "_id": _id,
                "_source": self.enrich_dates(i)
            })
        return dir_docs

    def update_directory(self, out):
        dir_docs = []
        if self.ref_date(out[0]['DateHalted']) >= self.max_date(self.INDEX):
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
