#!/usr/bin/env python

'''
    Scrape and ingest new otc transactions from FINRA otc bulletin board

    ** Note **
    This runs prospectively using the --most-recent argument
'''
import argparse

from modules.scrape_otc_raw import OTC_SCRAPE
from generic.generic_meta_enrich import GENERIC_META_ENRICH
from generic.logger import LOGGER


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ingest_otc')
    parser.add_argument('--date', type=str, dest='date', action="store")
    parser.add_argument('--expected', type=str, dest='expected', action="store")
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    parser.add_argument("--from-scratch",
                        action='store_true')
    parser.add_argument("--most-recent",
                        action='store_true')
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    parser.add_argument("--index",
                        type=str,
                        action='store')

    args = parser.parse_args()

    logger = LOGGER('scrape_otc', args.log_file).create_parent()

    logger.info('Starting otc scrape')
    otcs = OTC_SCRAPE(args)
    gme = GENERIC_META_ENRICH(args, 'scrape_otc')

    doc_count = otcs.main()
    gme.main(doc_count, 'ernest_otc_raw_cat')
    logger.info('Ending otc scrape')
