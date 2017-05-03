#!/usr/bin/env python2.7

import argparse

from enrich.generic.logger import LOGGER
from enrich.modules.compute_symbology import TO_SYMBOLOGY


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='to_symbology')
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--last-week',
                        dest='last_week',
                        action="store_true")
    parser.add_argument("--config-path",
                        type=str, action='store',
                        default='../config.json')
    args = parser.parse_args()
    logger = LOGGER('symbology', 'symbology.log').create_parent()
    ts = TO_SYMBOLOGY(args, 'symbology')
    ts.update_symbology('edgar')
