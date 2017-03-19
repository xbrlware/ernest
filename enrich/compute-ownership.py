#!/usr/bin/env python2.7

import argparse
import logging

from modules.compute_ownership_graph import COMPUTE_OWNERSHIP
from modules.compute_symbology import TO_SYMBOLOGY
from generic.generic_meta_enrich import GENERIC_META_ENRICH

if __name__ == "__main__":
    logger = logging.getLogger('compute_ownership')
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(description='grab_new_filings')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    parser.add_argument('--index',
                        type=str,
                        dest='index',
                        action="store")
    parser.add_argument('--date',
                        type=str,
                        dest='date',
                        action="store")
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--last-week',
                        dest='last_week',
                        action="store_true")
    parser.add_argument('--expected',
                        type=str,
                        dest='expected',
                        action="store")
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    args = parser.parse_args()

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

    cog = COMPUTE_OWNERSHIP(args)
    ts = TO_SYMBOLOGY(args, 'compute_ownership')
    gme = GENERIC_META_ENRICH(args)

    doc_count = cog.main()
    gme.main(doc_count, 'ernest_ownership_cat')
    ts.update_symbology('ownership')
