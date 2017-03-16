#!/usr/bin/env python

import json
import argparse
import re
from elasticsearch import Elasticsearch


class GENERIC_META_ENRICH:
    def __init__(self, args):
        self.month_lookup = {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5,
                             'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10,
                             'nov': 11, 'dec': 12}
        self.args = args
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config
            self.client = Elasticsearch([{"host": config['es']['host'],
                                          "port": config['es']['port']
                                          }], timeout=6000)
        if args.expected:
            self.expected = args.expected
        else:
            self.expected = None

    def __to_date(self, date):
        d = re.compile('\s{1}\d{1,2}\s{1}')
        d1 = re.findall(d, date)[0]
        d2 = re.sub('\D', '', d1).zfill(2)
        month = date[:7][4:].lower()
        month2 = str(self.month_lookup[month])
        month3 = month2.zfill(2)
        year = date[-4:]
        date2 = year + '-' + month3 + '-' + d2
        return date2

    def __build_out(self, doc_count):
        return {
            "index": self.args.index,
            "expected": self.expected,
            "count_in": doc_count[0] or self.args.count_in,
            "count_out": doc_count[1] or self.args.count_out,
            "date": self.__to_date(self.args.date)
        }

    def __build_id(self):
        idx = self.args.index
        dte = re.sub('-', '', self.__to_date(self.args.date))
        return idx + "__" + dte

    def main(self, doc_count):
        self.client.index(
            index='ernest_performance_graph2',
            doc_type='execution',
            id=self.__build_id(),
            body=self.__build_out(doc_count)
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generic Monitoring Index')
    parser.add_argument('--index', type=str, dest='index', action="store")
    parser.add_argument('--expected', type=str, dest='expected', action="store")
    parser.add_argument('--count-in', type=str, dest='count_in', action="store")
    parser.add_argument('--count-out',
                        type=str,
                        dest='count_out',
                        action="store")
    parser.add_argument('--date', type=str, dest='date', action="store")
    parser.add_argument('--config-path',
                        type=str,
                        action='store',
                        default='/home/ubuntu/ernest/config.json')

    gme = GENERIC_META_ENRICH(parser.parse_args())
    gme = gme.main()
