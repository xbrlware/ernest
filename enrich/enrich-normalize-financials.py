#!/usr/bin/env python

"""
    Scale year to date values in financials
    documents to quarter on quarter values
"""

import argparse
from generic.logger import LOGGER
from modules.enrich_normalize_financials import ENRICH_NORMALIZE_FINANCIALS


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='normalize_filings')
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--most-recent',
                        dest='most_recent',
                        action="store_true")
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    parser.add_argument("--log-file",
                        type=str,
                        action="store")
    args = parser.parse_args()
    logger = LOGGER('normalize_financials', args.log_file).create_parent()
    ENRICH_NORMALIZE_FINANCIALS(args, 'normalize_financials').enrich()
