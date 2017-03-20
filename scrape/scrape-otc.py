#!/usr/bin/env python

'''
    Scrape and ingest new otc transactions from FINRA otc bulletin board

    ** Note **
    This runs prospectively using the --most-recent argument
'''
import argparse
import logging

from modules.scrape_otc_raw import OTC_SCRAPE
from generic.generic_meta_enrich import GENERIC_META_ENRICH

if __name__ == "__main__":
    logger = logging.getLogger('scrape_otc')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='ingest_otc')
    parser.add_argument('--date', type=str, dest='date', action="store")
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
    args = parser.parse_args()

    logging.captureWarnings(True)
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

    logger.info('Starting otc scrape')
    otcs = OTC_SCRAPE(args)
    gme = GENERIC_META_ENRICH(args)

    doc_count = otcs.main()
    gme.main(doc_count, 'ernest_otc_raw_cat')
    logger.info('Ending otc scrape')
