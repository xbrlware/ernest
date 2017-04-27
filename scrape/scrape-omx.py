#!/usr/bin/env python2.7

import argparse

from modules.scrape_omx_html import OMX_SCRAPER
from enrich_modules.enrich_to_cik import TO_CIK
from generic.logger import LOGGER


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='omx_scraper')
    parser.add_argument('--start-page', type=int, action='store')
    parser.add_argument('--config-path', type=str, action='store')
    parser.add_argument('--halts',
                        dest='halts',
                        action="store_true")
    parser.add_argument("--index",
                        type=str, action='store',
                        required=True)
    parser.add_argument("--ticker-to-cik-field-name",
                        type=str,
                        action='store',
                        required=True)
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    args = parser.parse_args()

    logger = LOGGER('omx_scraper', args.log_file).create_parent()

    omxs = OMX_SCRAPER(args, 'omx_scraper')
    to_cik = TO_CIK(args, 'omx_scraper')

    omxs.main()
    to_cik.ticker_to_cik()
