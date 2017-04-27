#!/usr/bin/env python

"""
    Add fiscal period information to 10k and 10q filings in aq-forms index

"""
import argparse
from generic.logger import LOGGER
from modules.enrich_aqfs_fye import ENRICH_AQFS_FYE


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='add_fye_info')
    parser.add_argument('--sub', dest='sub', action="store_true")
    parser.add_argument('--function', dest='function', action="store_true")
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='/home/ubuntu/ernest/config.json')
    parser.add_argument("--log-file", type=str, action="store")

    args = parser.parse_args()
    logger = LOGGER('aqfs_fye', args.log_file).create_parent()
    ENRICH_AQFS_FYE(args, 'aqfs_fye').enrich()
