#!/usr/bin/env python

'''
    1.) Update documents in ernest_nt_filings index
    2.) Add period to NT filings documents in ernest_nt_filings
    3.) Add enrich tag for filings that have NT documents in the edgar index

    Note:
        Runs prospectively using the most-recent argument

'''

import argparse

from generic.logger import LOGGER
from modules.build_nt_filings import BUILD_NT_FILINGS


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='add_nt_docs')
    parser.add_argument('--from-scratch',
                        dest='from_scratch', action="store_true")
    parser.add_argument('--most-recent',
                        dest='most_recent', action="store_true")
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='/home/ubuntu/ernest/config.json')
    parser.add_argument("--log-file",
                        type=str,
                        action='store')
    args = parser.parse_args()
    logger = LOGGER('nt_filings', args.log_file).create_parent()
    BUILD_NT_FILINGS(args, 'nt_filings').main()
