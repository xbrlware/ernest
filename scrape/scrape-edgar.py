#!/usr/bin/env python2.7

import argparse
import logging

from datetime import date
from modules.scrape_edgar_index import EDGAR_INDEX
from modules.scrape_edgar_forms import EDGAR_INDEX_FORMS
from enrich.generic_meta_enrich import GENERIC_META_ENRICH

if __name__ == "__main__":
    logger = logging.getLogger('scrape_edgar')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='Scrape EDGAR indices')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    parser.add_argument('--index',
                        type=str,
                        dest='index',
                        action="store")
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
    parser.add_argument("--back-fill",
                        action='store_true')
    parser.add_argument('--date',
                        type=str,
                        dest='date',
                        action="store")
    parser.add_argument("--start-date",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument("--end-date",
                        type=str,
                        action='store',
                        default=date.today().strftime('%Y-%m-%d'))
    parser.add_argument("--section",
                        type=str,
                        action='store',
                        default='both')
    parser.add_argument("--form-types",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument('--expected',
                        type=str,
                        dest='expected',
                        action="store")
    parser.add_argument('--config-path',
                        type=str,
                        action='store',
                        default='../config.json')

    args = parser.parse_args()

    fh = logging.FileHandler(args.log_file)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '[%(asctime)s] [%(name)s] [%(levelname)s] :: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

    ei = EDGAR_INDEX(args)
    eif = EDGAR_INDEX_FORMS(args)
    gme = GENERIC_META_ENRICH(args)

    logger.info('edgar index begin')
    doc_count = ei.main()
    gme.main(doc_count, 'edgar_index_cat')
    logger.info('edgar index end')

    logger.info('edgar forms begin')
    doc_count = eif.main()
    gme.main(doc_count, 'edgar_forms_cat')
    logger.info('edgar forms end')
