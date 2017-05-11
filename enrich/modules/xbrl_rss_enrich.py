#!/usr/bin/env python

import calendar
import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan


class XBRL_RSS_ENRICH:
    def __init__(self, args, parent_logger):
        with open(args.config_path, 'rb') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{"host": config['es']['host'],
                                      "port": config['es']['port']}])
        self.logger = logging.getLogger(parent_logger + '.enrich')
        if not args.month:
            from_date = str(args.year) + '-01-01'
            to_date = str(args.year) + '-12-31'
        elif args.month:
            days = calendar.monthrange(int(args.year), int(args.month))
            from_date = str(args.year) + '-' + str(args.month).zfill(2) + '-01'
            to_date = str(args.year) + '-' \
                + str(args.month).zfill(2) + '-' + str(days[1]).zfill(2)

        self.query = {
            "query": {
                "range": {
                    "date": {
                        "gte": from_date,
                        "lte": to_date
                    }
                }
            }
        }

        self.INDEX = config['aq_forms_enrich']['index']
        self.REF_INDEX = config['xbrl_rss']['index']
        self.TYPE = config['aq_forms_enrich']['_type']

    def run(self, query):
        for a in scan(self.client, index=self.INDEX, query=query):
            try:
                res = self.client.search(index=self.REF_INDEX, body={
                    "query": {
                        "bool": {
                            "must": [{
                                "match": {
                                    "entity_info.dei_EntityCentralIndexKey.fact":
                                    a["_source"]["cik"].zfill(10)
                                    }
                                },
                                {
                                "match": {
                                    "entity_info.dei_DocumentType.to_date":
                                    a["_source"]["_enrich"]["period"]
                                    }
                                }
                            ]
                        }
                    }
                })
            except KeyError:
                self.logger.error('{0}|{1}'.format('KEYERROR', a))

            if res['hits']['total'] > 0:
                body = res['hits']['hits'][0]['_source']['facts']
                doc = {
                    "__meta__": {
                        "financials": self.get_financials(body)
                        }
                }
            else:
                doc = {
                    "__meta__": {
                        "financials": None
                    }
                }
            yield {
                "_id": a['_id'],
                "_type": self.TYPE,
                "_index": self.INDEX,
                "_op_type": "update",
                "doc": doc
            }

    def to_numeric(self, val):
        if val is not None:
            if val['value'] == 'NA':
                val['value'] = 0
            val['value'] = float(val['value'])

        return self.fix_dates(val)

    def fix_dates(self, val):
        if val is not None:
            if val['to'] == 'NA':
                val['to'] = None
            if val['from'] == 'NA':
                val['from'] = None
        return val

    def get_financials(self, body):
        out = {
            'assets': self.to_numeric(body.get("us-gaap_Assets", None)),
            'liabilities': self.to_numeric(
                body.get("us-gaap_Liabilities", None)),
            'stockholdersEquity': self.to_numeric(
                body.get("us-gaap_StockholdersEquity", None)),
            'netIncome': self.to_numeric(
                body.get("us-gaap_NetIncomeLoss", None)),
            'liabilitiesAndStockholdersEquity': self.to_numeric(
                body.get("us-gaap_LiabilitiesAndStockholdersEquity", None)),
            'liabilitiesCurrent': self.to_numeric(
                body.get("us-gaap_LiabilitiesCurrent", None)),
            'assetsCurrent': self.to_numeric(
                body.get("us-gaap_AssetsCurrent", None)),
            'revenues': self.to_numeric(body.get("us-gaap_Revenues", None)),
            'commonStockValue': self.to_numeric(
                body.get("us-gaap_CommonStockValue", None)),
            'commonStockSharesOutstanding': self.to_numeric(
                body.get("us-gaap_CommonStockSharesOutstanding", None)),
            'commonStockSharesIssued': self.to_numeric(
                body.get("us-gaap_CommonStockSharesIssued", None)),
            'operatingIncome': self.to_numeric(
                body.get("us-gaap_OperatingIncomeLoss", None)),
            'accountsPayable': self.to_numeric(
                body.get("us-gaap_AccountsPayableCurrent", None)),
            'cash': self.to_numeric(
                body.get("us-gaap_CashAndCashEquivalentsAtCarryingValue",
                         body.get('us-gaap_Cash', None))),
            'interestExpense': self.to_numeric(
                body.get("us-gaap_InterestExpense", None)),
            'operatingExpense': self.to_numeric(
                body.get("us-gaap_OperatingExpenses", None)),
            'earnings': self.to_numeric(
                body.get("us-gaap_RetainedEarningsAccumulatedDeficit", None)),
            'profit': self.to_numeric(
                body.get("us-gaap_ProfitLoss",
                         body.get('us-gaap_GrossProfit', None)))
        }
        return out

    def main(self):
        for a, b in streaming_bulk(self.client, self.run(self.query),
                                   raise_on_error=False):
            self.logger.info('{0}|{1}'.format('UPDATED', b['update']['_id']))
