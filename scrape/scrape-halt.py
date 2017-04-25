#!/usr/bin/env python2.7

import argparse

from datetime import datetime
from generic.logger import LOGGER
from modules.scrape_sec_suspensions import SEC_SCRAPER
from enrich_modules.merge_halts import MERGE_HALTS
from enrich_modules.enrich_to_cik import TO_CIK


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
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--date',
                        type=str,
                        dest='date',
                        action="store")
    parser.add_argument('--expected',
                        type=str,
                        dest='expected',
                        action="store")
    parser.add_argument("--index",
                        type=str, action='store',
                        required=True)
    parser.add_argument("--name-to-cik-field-name",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument("--ticker-to-cik-field-name",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument("--threshold",
                        type=float,
                        action='store',
                        default=7)
    parser.add_argument('--halts',
                        dest='halts',
                        action="store_true")

    args = parser.parse_args()

    logger = LOGGER('scrape_halt', args.log_file).create_parent()

    secs = SEC_SCRAPER(args)
    mh = MERGE_HALTS(args, 'scrape_halt')
    to_cik = TO_CIK(args, 'scrape_halt')

    doc_count = secs.main()
    mh.main()
    to_cik.main()
