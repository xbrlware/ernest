#!/usr/bin/env python2.7

import argparse

from datetime import date
from modules.scrape_edgar_index import EDGAR_INDEX
from modules.scrape_edgar_forms import EDGAR_INDEX_FORMS
from generic.generic_meta_enrich import GENERIC_META_ENRICH
from generic.logger import LOGGER
from enrich_modules.compute_symbology import TO_SYMBOLOGY


def main():
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
    parser.add_argument('--last-week',
                        dest='last_week',
                        action="store_true")

    args = parser.parse_args()

    logger = LOGGER('scrape_edgar', args.log_file).create_parent()

    ei = EDGAR_INDEX(args, 'scrape_edgar')
    eif = EDGAR_INDEX_FORMS(args, 'scrape_edgar')
    gme = GENERIC_META_ENRICH(args, 'scrape_edgar')
    ts = TO_SYMBOLOGY(args, 'scrape_edgar')

    logger.info('[EDGAR]|begin indexing')
    doc_count = ei.main()
    gme.main(doc_count, 'edgar_index_cat')
    logger.info('[EDGAR]|indexing ended')

    logger.info('[EDGARFORMS]|begin indexing')
    doc_count = eif.main()
    gme.main(doc_count, 'edgar_forms_cat')
    logger.info('[EDGARFORMS]|forms indexing ended')

    ts.update_symbology('edgar')

if __name__ == "__main__":
    main()
