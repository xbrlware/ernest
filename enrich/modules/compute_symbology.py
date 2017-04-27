'''
    Create symbology index from forms index
'''

import argparse
import json
import logging

from hashlib import sha1
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk


class TO_SYMBOLOGY:
    def __init__(self, args, logging_parent):
        self.logger = logging.getLogger(logging_parent + '.to_symbology')
        self.args = args
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.edgar_query = {
            "_source": ["cik", "date", "name"],
            "query": {
                "match_all": {}
            }
        }

        self.ownership_query = {
            "_source": [
                "ownershipDocument.issuer",
                "ownershipDocument.periodOfReport",
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
                                        "field": "ownershipDocument.\
                                        issuer.issuerCik"
                                    }
                                }
                            }
                        }
                    ]
                }
            }
        }

        if args.last_week:
            self.edgar_query = {
                "query": {
                    "range": {
                        "date": {
                            "gte": str(date.today() - timedelta(days=9))
                        }
                    }
                }
            }

            self.ownership_query['query']['bool']['must'].append({
                "range": {
                    "ownershipDocument.periodOfReport": {
                        "gte": str(date.today() - timedelta(days=9))
                    }
                }
            })

        elif not args.from_scratch:
            self.logger.critical('[MISSINGFLAG]|--from-scratch or --last-week')

        self.client = Elasticsearch([{
            'host': config["es"]["host"],
            'port': config["es"]["port"]
        }], timeout=60000)

    def get_id(self, x):
        return sha1('__'.join(map(str, x[0]))).hexdigest()

    def merge_dates(self, x, min_dates):
        id_ = self.get_id(x)
        if min_dates.get(id_, False):
            x[1]['min_date'] = min_dates[id_]
        return x

    def get_properties(self, x, prop_type):
        if prop_type == 'edgar':
            tmp = {
                "cik": str(x['cik']).zfill(10),
                "name": x['name'].encode('utf-8').upper(),
                "sic": None,
                "ticker": None,
                "min_date": str(x['date']),
                "max_date": str(x['date'])
            }
        elif prop_type == 'ownership':
            try:
                sic = x[1]
                ['header']['ISSUER'][0]['COMPANY-DATA'][0]['ASSIGNED-SIC'][0]
            except (KeyError, IndexError):
                sic = None

            tmp = {
                "cik": str(
                    x[1]['ownershipDocument']['issuer']['issuerCik']).zfill(10),
                "name": str(
                    x[1]['ownershipDocument']['issuer']['issuerName']).upper(),
                "ticker": str(
                    x[1]['ownershipDocument']['issuer']['issuerTradingSymbol']
                ).upper(),
                "sic": sic,
                "min_date": str(x[1]['ownershipDocument']['periodOfReport']),
                "max_date": str(x[1]['ownershipDocument']['periodOfReport'])
            }

        id_tuple = (
            (tmp['cik'], tmp['name'], tmp['ticker'], tmp['sic']),
            (tmp['min_date'], tmp['max_date'])
        )

        tmp['id'] = self.get_id(id_tuple)
        return tmp

    def coerce_out(self, x, co_type, op_type):
        # co_type is either "index" or "ownership"
        x["__meta__"] = {"source": co_type}
        ro = {
            '_op_type': op_type,
            '_id': x['id'],
            '_index': self.config['symbology']['index'],
            '_type': self.config['symbology']['_type'],
            'doc': x
        }
        return ro

    def update_symbology(self, u_type):
        self.logger.info('[STARTING]|updating symbology from {} index'.format(
            u_type))
        update_list = []
        if u_type == 'edgar':
            idx = self.config['edgar_index']['index']
            dt = self.config['edgar_index']['_type']
            q = self.edgar_query
        elif u_type == 'ownership':
            idx = self.config['forms']['index']
            dt = self.config['forms']['_type']
            q = self.ownership_query

        i = 0
        for doc in scan(self.client, index=idx, doc_type=dt, query=q):
            pdoc = self.get_properties(doc['_source'], u_type)
            try:
                mtc = self.client.get(
                    index=self.config['symbology']['index'],
                    doc_type=self.config['symbology']['_type'],
                    id=pdoc['id'])
                pdoc['min_date'] = mtc['_source']['min_date']
                update_list.append(self.coerce_out(pdoc, u_type, 'update'))
            except:
                self.logger.info('[MISSING]|{}'.format(pdoc['id']))
                update_list.append(self.coerce_out(pdoc, u_type, 'index'))

            if i > 499:
                for a, b in parallel_bulk(self.client, update_list):
                    if a is not True:
                        self.logger.error('[ELASTICSEARCH]|{0}, {1}'.format(
                            a, b))

                update_list = []
                i = 0
            i += 1

        for a, b in parallel_bulk(self.client, update_list):
            if a is not True:
                self.logger.error('[ELASTICSEARCH]|{0}, {1}'.format(a, b))

        self.logger.info(
            '[DONE]|updated symbology from {} index'.format(u_type))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='to_symbology')
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--last-week',
                        dest='last_week',
                        action="store_true")
    parser.add_argument("--config-path",
                        type=str, action='store',
                        default='../config.json')
    args = parser.parse_args()
    ts = TO_SYMBOLOGY(args)
    ts.main()
