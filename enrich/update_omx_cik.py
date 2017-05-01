#!/usr/bin/env python

from generic.logger import LOGGER
from modules.enrich_to_cik import TO_CIK


class args:
    config_path = '../config.json'
    ticker_to_cik_field_name = "tickers.symbol.cat"
    index = "omx"
    halts = None


logger = LOGGER('update_omx_cik', 'omx_cik.log').create_parent()
tcik = TO_CIK(args, 'update_omx_cik')
tcik.ticker_to_cik()
