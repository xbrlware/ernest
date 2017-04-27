#!/usr/bin/env python

'''
scrape xbrl files

'''

import argparse

from modules.scrape_xbrl_submissions import SCRAPE_XBRL_SUBMISSIONS
from generic.logger import LOGGER


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ingest_new_forms')
    parser.add_argument("--from-scratch", action='store_true')
    parser.add_argument("--most-recent", action='store_true')
    parser.add_argument("--period", type=str, action='store', default='False')
    parser.add_argument("--config-path",
                        type=str, action='store',
                        default='../config.json')
    parser.add_argument("--log-file", action='store', type=str)

    args = parser.parse_args()

    logger = LOGGER('scrape_xbrl', args.log_file).create_parent()

    sxs = SCRAPE_XBRL_SUBMISSIONS(args, 'scrape_xbrl')
    sxs.main()
