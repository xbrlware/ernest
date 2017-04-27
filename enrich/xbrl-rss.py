#!/usr/bin/env python

import argparse

from generic.logger import LOGGER
from modules.xbrl_rss_enrich import XBRL_RSS_ENRICH
from modules.xbrl_rss_interpolation import XBRL_RSS_INTERPOLATION


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='enrich-xbrl-rss-docs')
    parser.add_argument("--year",  type=str, action='store')
    parser.add_argument("--month",  type=str, action='store')
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='/home/ubuntu/ernest/config.json')
    parser.add_argument("--log-file",
                        type=str,
                        action="store",
                        required=True)
    args = parser.parse_args()
    logger = LOGGER('xbrl_rss', args.log_file).create_parent()
    xre = XBRL_RSS_ENRICH(args, 'xbrl_rss')
    xri = XBRL_RSS_INTERPOLATION(args, 'xbrl_rss')
    xre.main()
    xri.main()
