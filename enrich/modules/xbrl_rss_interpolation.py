#!/usr/bin/env python

'''
    Interpolates high level financials using basic accounting identities when
    those tags are not made explicit in a given xbrl filing

    ** Note **
    This runs prospectively each day after new xbrl filings are downloaded,
    parsed and ingested
'''

import argparse
import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from generic.logger import LOGGER


class XBRL_RSS_INTERPOLATION:
    def __init__(self, args, parent_logger):
        with open(args.config_path, 'rb') as inf:
            config = json.load(inf)
            self.config = config
        self.client = Elasticsearch([{"host": config['es']['host'],
                                      "port": config['es']['port']}])
        self.logger = logging.getLogger(parent_logger + '.interpolation')
        self.query = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "filtered": {
                                    "filter": {
                                        "exists": {
                                            "field": "__meta__.financials"
                                            }
                                        }}
                                    },
                            {
                                "filtered": {
                                    "filter": {
                                        "missing": {
                                            "field":
                                            "__meta__.financials.interpolated"
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }

    def __value(self, a, b, c, add_subt):
        """ x = a, b, or 0. y = c or 0. val = x (- or +) y """
        if a is not None:
            x = a['value']
        else:
            if b is not None:
                x = b
            else:
                x = 0
        if c is not None:
            y = c['value']
        else:
            y = 0
        if add_subt == '+':
            val = x + y
        elif add_subt == '-':
            val = x - y
        return {"value": val}

    def interpolate(self, a):
        doc = a['_source']['__meta__']['financials']
        for k, v in doc.iteritems():
            if v is not None:
                continue
            if k == 'assets':
                doc[k] = self.__value(doc['liabilitiesAndStockholdersEquity'],
                                      doc['liabilities'],
                                      doc['stockholdersEquity'],
                                      '+')
            elif k == 'liabilities':
                doc[k] = self.__value(doc['liabilitiesAndStockholdersEquity'],
                                      doc['assets'],
                                      doc['stockholdersEquity'],
                                      '-')
            elif k == 'stockholdersEquity':
                doc[k] = self.__value(doc['liabilitiesAndStockholdersEquity'],
                                      doc['assets'],
                                      doc['liabilities'],
                                      '-')
            elif k == 'liabilitiesAndStockholdersEquity':
                doc[k] = self.__value(doc['assets'],
                                      doc['liabilities'],
                                      doc['stockholdersEquity'],
                                      '+')
            else:
                pass

        doc['interpolated'] = True
        return a

    def main(self):
        for a in scan(self.client,
                      index=self.config['aq_forms_enrich']['index'],
                      query=self.query):
            s = self.interpolate(a)
            res = self.client.index(
                index=self.config['aq_forms_enrich']['index'],
                doc_type=self.config['aq_forms_enrich']['_type'],
                body=s['_source'],
                id=s['_id']
            )
            self.logger.info('{0}|{1}'.format('INDEXED', res['_id']))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ingest-xbrl-rss-docs')
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='/home/ubuntu/ernest/config.json')
    parser.add_argument("--log-file",
                        type=str,
                        action="store",
                        required=True)
    args = parser.parse_args()
    logger = LOGGER('xbrl_rss', args.log_file)
    xri = XBRL_RSS_INTERPOLATION(args, 'xbrl_rss')
