#!/usr/bin/env python

'''
Download new AQFS submissions from the EDGAR AQFS financial datasets page

** Note **
This runs prospectively using the --most-recent argument
'''

import argparse
import logging
import json

from datetime import date
from elasticsearch import Elasticsearch

from http_handler import HTTP_HANDLER


class SCRAPE_XBRL_SUBMISSIONS:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + '.submissions')
        self.hh = HTTP_HANDLER(parent_logger + '.submissions')
        self.session = self.hh.create_session()

        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']
        }])

    def ingest(self, period):
        self.logger.info('[INGESTING]|{}'.format(period))
        u = 'https://www.sec.gov/data/financial-statements/' + period + '.zip'
        aqfs = self.hh.get_xbrl_sub(self.session, u)

        if aqfs:
            lst = []
            for line in aqfs:
                row = line.split('\t')
                row[35] = row[35].replace('\n', '')
                lst.append(row)

            for i in range(1, len(lst)):
                x = lst[0]
                y = lst[i]
                dictionary = dict(zip(x, y))
                dictionary['file_period'] = period

                res = self.client.index(index="xbrl_submissions_cat",
                                        doc_type='filing',
                                        body=dictionary,
                                        id=dictionary['adsh'])
                self.logger.info('[RESPONSE]|{}'.format(res))

    def s_filter(self, yr):
        if yr < date.today().year:
            for qtr in range(1, 5):
                return str(yr) + 'q' + str(qtr)

        elif yr == date.today().year:
            for qtr in range(1, (int(date.today().month) / 3) + 1):
                return str(yr) + 'q' + str(qtr)

    def main(self):
        p = []

        if self.args.from_scratch:
            p = [self.s_filter(yr) for yr in (range(
                2009, int(date.today().year) + 1))]

        elif self.args.most_recent:
            qtr = str(int(date.today().month) / 3)
            if qtr < 1:
                self.logger.warning('[NODATA]|no data for first quarter')
            else:
                yr = str(int(date.today().year)) + 'q' + qtr
                p.append(yr)

        elif self.args.period == 'False':
            self.logger.error(
                '[ARGUMENTS]|--period, --from-scratch, or --most-recent')

        else:
            p.append(self.args.period)

        for period in p:
            self.ingest(period)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ingest_new_forms')
    parser.add_argument("--from-scratch", action='store_true')
    parser.add_argument("--most-recent", action='store_true')
    parser.add_argument("--period", type=str, action='store', default='False')
    parser.add_argument("--config-path",
                        type=str, action='store',
                        default='../config.json')
    parser.add_argument("--log-file", action='store', type=str)

    args = parser.parse_args()

    sxs = SCRAPE_XBRL_SUBMISSIONS(args)
    sxs.main()
