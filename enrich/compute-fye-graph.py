#!/usr/bin/env python

'''
    Aggregate fiscal year end schedule for each company in
    the ernest_aq_forms index

    ** Note **
    This runs prospectively using the --most-recent argument
'''

import argparse

from modules.compute_fye import COMPUTE_FYE
from generic.logger import LOGGER

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='fye_aggregation')
    parser.add_argument('--from-scratch',
                        dest='from_scratch', action="store_true")
    parser.add_argument('--most-recent',
                        dest='most_recent', action="store_true")
    parser.add_argument("--config-path", type=str,
                        action='store', default='../config.json')
    parser.add_argument("--log-file", type=str, action="store")
    args = parser.parse_args()
    logger = LOGGER('fye_graph', args.log_file).create_parent()
    COMPUTE_FYE(args, 'fye_graph').compute()
