#!/usr/bin/env python

'''
    Scrape and ingest new otc halts, delinquencies and companies from finra otc
    json pages

    ** Note **
    This runs accoring to the directory specified in the --directory argument:
        --directory = 'halts'
        --directory = 'delinquency'
        --directory = 'directory'
'''

import argparse
import logging
from generic.generic_meta_enrich import GENERIC_META_ENRICH
from modules.scrape_finra_directories import FINRA_DIRS

if __name__ == "__main__":
    logger = logging.getLogger('scrape_finra')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='ingest_finra_docs')
    parser.add_argument("--directory",
                        type=str, action='store')
    parser.add_argument("--update-halts",
                        action='store_true')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
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

    if args.directory:
        gme_str = 'ernest_otc_directory_cat'
    elif args.delinquency:
        gme_str = 'ernest_otc_delinquency_cat'

    gme = GENERIC_META_ENRICH(args, 'scrape_finra')
    fd = FINRA_DIRS(args)
    doc_count = fd.main()

    gme.main(doc_count, gme_str)
