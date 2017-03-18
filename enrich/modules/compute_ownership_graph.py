#!/usr/bin/env python2.7

'''
    Create ownership index from edgar_index index

    ** Note **
    This runs prospectively using the --last-week parameter
    to grab docs retrospectively for the previous nine days
'''

import json
import logging
import re

from operator import itemgetter
from itertools import chain, groupby
from datetime import datetime
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk


class COMPUTE_OWNERSHIP:
    def __init__(self, args):
        self.logger = logging.getLogger('compute_ownership.graph')
        self.args = args
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.query = {
            "_source": [
                "ownershipDocument.periodOfReport",
                "ownershipDocument.reportingOwner",
                "ownershipDocument.issuer",
                "header.ACCESSION-NUMBER",
                "header.ISSUER.COMPANY-DATA.ASSIGNED-SIC"
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "filtered": {
                                "filter": {
                                    "exists": {
                                        "field": "ownershipDocument"}}}}]}}}

        if args.last_week:
            self.query['query']['bool']['must'].append({
                "range": {
                    "ownershipDocument.periodOfReport": {
                        "gte": str(date.today() - timedelta(days=9))}}})
        elif not args.from_scratch:
            raise Exception('must chose either --from-scratch or --last-week')

        self.client = Elasticsearch([{
            'host': config["es"]["host"],
            'port': config["es"]["port"]
        }], timeout=60000)

    def get_id(self, x):
        front = '__'.join(map(lambda x: re.sub(' ', '_', str(x)), x[:9]))
        try:
            id = front + '__' + x[9]['COMPANY-DATA'][0]['ASSIGNED-SIC'][0]
        except:
            id = front + "__0000"

        return (id,) + x

    def clean_logical(self, x):
        tmp = str(x).lower()
        if tmp == 'true':
            return 1
        elif tmp == 'false':
            return 0
        else:
            return x

    def _get_owners(self, r):
        return {
            "isOfficer": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isOfficer', 0)),
            "isTenPercentOwner": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isTenPercentOwner', 0)),
            "isDirector": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isDirector', 0)),
            "isOther": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isOther', 0)),
            "ownerName": self.clean_logical(
                r.get('reportingOwnerId',
                      {}).get('rptOwnerName', 0)),
            "ownerCik": self.clean_logical(
                r.get('reportingOwnerId',
                      {}).get('rptOwnerCik', 0))
        }

    def get_owners(self, val):
        try:
            sic = val['header']['ISSUER'][0]
            ['COMPANY-DATA'][0]
            ['ASSIGNED-SIC'][0]
        except (KeyError, IndexError):
            sic = None
        top_level_fields = {
            "issuerCik": val['ownershipDocument']['issuer']['issuerCik'],
            "issuerName": val['ownershipDocument']['issuer']['issuerName'],
            "issuerTradingSymbol": val['ownershipDocument']['issuer']
            ['issuerTradingSymbol'],

            "periodOfFiling": val['ownershipDocument']['periodOfReport'],
            "sic": sic
        }
        ro = val['ownershipDocument']['reportingOwner']
        ro = [ro] if isinstance(ro, dict) else ro
        ros = map(self._get_owners, ro)
        for r in ros:
            r.update(top_level_fields)
        return ros

    def get_properties(self, x):
        tmp = {
            "issuerCik": str(x['issuerCik']).zfill(10),
            "issuerName": str(x['issuerName']).upper(),
            "issuerTradingSymbol": str(x['issuerTradingSymbol']).upper(),
            "ownerName": str(x['ownerName']).upper(),
            "ownerCik": str(x['ownerCik']).zfill(10),
            "isDirector": int(x['isDirector']),
            "isOfficer": int(x['isOfficer']),
            "isOther": int(x['isOther']),
            "isTenPercentOwner": int(x['isTenPercentOwner']),
            "periodOfFiling": str(x['periodOfFiling']),
            "sic": x['sic'],
            "min_date": x['periodOfFiling'],
            "max_date": x['periodOfFiling']
        }
        return (
            (tmp['issuerCik'],
             tmp['issuerName'],
             tmp['issuerTradingSymbol'],
             tmp['ownerName'],
             tmp['ownerCik'],
             tmp['isDirector'],
             tmp['isOfficer'],
             tmp['isOther'],
             tmp['isTenPercentOwner'],
             tmp['sic'],
             tmp['min_date'],
             tmp['max_date']),
            tmp['periodOfFiling']
        )

    def coerce_out(self, x):
        return {
            "_op_type": "index",
            "_index": self.config["ownership"]["index"],
            "_type": self.config["ownership"]["_type"],
            "_id": str(x[0]),
            "doc": {
                "issuerCik": str(x[1]),
                "issuerName": str(x[2]),
                "issuerTradingSymbol": str(x[3]),
                "ownerName": str(x[4]),
                "ownerCik": str(x[5]),
                "isDirector": int(x[6]),
                "isOfficer": int(x[7]),
                "isOther": int(x[8]),
                "isTenPercentOwner": int(x[9]),
                "sic": x[10],
                "min_date": str(x[11]),
                "max_date": str(x[12])
            }
        }

    def find_max_date(self, l):
        max_date = datetime.strptime(l[0][11], "%Y-%m-%d")
        for ele in l[1:]:
            t = datetime.strptime(l[0][11], "%Y-%m-%d")
            if t > max_date:
                max_date = t
        return l[0][:11] + (max_date.strftime("%Y-%m-%d"),)

    def make_list(self, g):
        z = [x for x, y in g]
        return self.find_max_date(z)

    def main(self):
        self.logger.info('Starting compute-ownership-graph')

        resp = self.client.count(index=self.config['ownership']['index'])
        count_in = resp['count'] or None

        self.logger.debug('get_owners')
        df = list(chain.from_iterable(
            [self.get_owners(a['_source']) for a in scan(
                self.client,
                index=self.config['forms']['index'],
                doc_type=self.config['forms']['_type'],
                query=self.query)]))
        self.logger.debug('get_properties')
        df2 = [self.get_properties(d) for d in df]
        df2.sort(key=itemgetter(0))
        self.logger.debug('make_list')
        df3 = [self.make_list(g) for k, g in groupby(df2, key=itemgetter(0))]
        self.logger.debug('get_id')
        df4 = [self.get_id(d) for d in df3]

        if self.args.last_week:
            self.logger.info('checking if document in ownership index')

            for i in df4:
                try:
                    mtc = self.client.get(
                        index=self.config['ownership']['index'],
                        doc_type=self.config['ownership']['_type'],
                        id=str(i[0]))
                    i = i[:11] + (mtc['_source']['doc']['min_date'],) + (i[12],)
                except:
                    self.logger.info('missing \t %s' % i[0])

        self.logger.info('indexing documents')
        for a, b in parallel_bulk(self.client,
                                  [self.coerce_out(d) for d in df4]):
            self.logger.info('{}'.format(b))

        resp = self.client.count(index=self.config['ownership']['index'])
        count_out = resp['count'] or None

        self.logger.info('%d in, %d out' % (count_in, count_out))
        return [count_in, count_out]
