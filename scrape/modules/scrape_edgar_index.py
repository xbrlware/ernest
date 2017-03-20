#!/usr/bin/env python2.7

'''
    Download new edgar filings documents to edgar_index_cat

    ** Note **
    This runs prospectively using the --most-recent argument
'''

import argparse
import json
import logging
import urllib2

from datetime import datetime, date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


class EDGAR_INDEX:
    def __init__(self, args):
        self.args = args
        self.logger = logging.getLogger('scrape_edgar.edgar_index')
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.client = Elasticsearch([{"host": config['es']['host'],
                                          "port": config['es']['port']}],
                                        timeout=6000)
            self.config = config

    def __handle_url(self, url):
        return urllib2.urlopen(url)

    def __get_max_date(self):
        query = {"size": 0, "aggs": {"max": {"max": {"field": "date"}}}}
        d = self.client.search(index=self.config['edgar_index']['index'],
                               body=query)
        return int(d['aggregations']['max']['value'])

    def __parse_line(self, line, from_date):
        cik, name, form, date, url = line.strip().split('|')
        di = 1000 * int(datetime.strptime(date, '%Y-%m-%d').strftime("%s"))
        if di <= from_date:
            return None

        return {"_id": url,
                "_type": self.config['edgar_index']['_type'],
                "_index": self.config['edgar_index']['index'],
                "_source": {
                    "cik": cik,
                    "name": (name.replace("\\", '')).decode('unicode_escape'),
                    "form": form,
                    "date": date,
                    "url": url
                }}

    def __download_index(self, yr, q, from_date):
        parsing = False
        base_url = "https://www.sec.gov/Archives/edgar/full-index"
        url = '%s/%d/QTR%d/master.idx' % (base_url, yr, q)
        page = self.__handle_url(url)
        for line in page:
            if parsing:
                parsed_line = self.__parse_line(line, from_date)
                if parsed_line:
                    yield parsed_line
                else:
                    pass

            elif line[0] == '-':
                parsing = True

    def main(self):
        if self.args.most_recent:
            year = [date.today().year]
            qtr = [((date.today().month - 1) / 3) + 1]
            from_date = self.__get_max_date()
        elif self.args.from_scratch:
            year = range(self.args.min_year, self.args.max_year)
            qtr = [1, 2, 3, 4]
            from_date = -1
        else:
            raise Exception(
                'Specificy argument --most-recent or --from-scratch')

        resp = self.client.count(index=self.config['edgar_index']['index'])
        count_in = resp['count'] or None

        for yr in year:
            for q in qtr:
                for a, b in streaming_bulk(self.client,
                                           self.__download_index(yr,
                                                                 q,
                                                                 from_date),
                                           chunk_size=1000):
                    self.logger.info('%s %s' % (a, b))

        resp = self.client.count(index=self.config['edgar_index']['index'])
        count_out = resp['count'] or None

        self.logger.info("%d in, %d out" % (count_in, count_out))

        return [count_in, count_out]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape EDGAR indices')
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--min-year',
                        type=int,
                        dest='min_year',
                        action="store",
                        default=2011)
    parser.add_argument('--max-year',
                        type=int,
                        dest='max_year',
                        action="store",
                        default=int(date.today().year))
    parser.add_argument('--most-recent',
                        dest='most_recent',
                        action="store_true")
    parser.add_argument('--config-path',
                        type=str,
                        action='store',
                        default='../config.json')

    args = parser.parse_args()
    ei = EDGAR_INDEX(args)
    ei.main()
