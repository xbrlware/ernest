#!/usr/bin/env python

"""
    Calculate delinquency for each new 10-K/10-Q filing based on the fiscal
    period end date and the filing status of the company

    ** Note **
    This runs prospectively each day after new filings have been downloaded
"""

import argparse

from modules.compute_delinquency import COMPUTE_DELINQUENCY
from generic.logger import LOGGER


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='/home/ubuntu/ernest/config.json')
    parser.add_argument("--log-file",
                        type=str,
                        action='store')
    args = parser.parse_args()
    logger = LOGGER('delinquency', args.log_file).create_parent()
    COMPUTE_DELINQUENCY(args, 'delinquency').compute_delinquency()
