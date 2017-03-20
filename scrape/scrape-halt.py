#!/usr/bin/env python2.7

import argparse

from datetime import datetime
from generic.generic_meta_enrich import GENERIC_META_ENRICH
from generic.logger import LOGGER
from modules.scrape_sec_suspensions import SEC_SCRAPER

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='scrape_trade_suspensions')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    parser.add_argument("--config-path",
                        type=str, action='store',
                        default='../config.json')
    parser.add_argument("--start-date",
                        type=str,
                        action='store',
                        default="2004-01-01")
    parser.add_argument("--end-year",
                        type=int,
                        action='store',
                        default=datetime.now().year)
    parser.add_argument("--stdout",
                        action='store_true')
    parser.add_argument("--most-recent",
                        action='store_true')
    parser.add_argument('--date',
                        type=str,
                        dest='date',
                        action="store")
    parser.add_argument('--expected',
                        type=str,
                        dest='expected',
                        action="store")
    parser.add_argument('--index',
                        type=str,
                        dest='index',
                        action="store")

    args = parser.parse_args()

    logger = LOGGER('scrape_halt', args.log_file).create_parent()

    gme = GENERIC_META_ENRICH(args, 'scrape_halt')
    secs = SEC_SCRAPER(args)

    doc_count = secs.main()
    gme.main(doc_count, 'ernest_sec_finra_halts')
