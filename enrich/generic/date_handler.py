#!/usr/bin/env python2.7

import re

from datetime import datetime


class DATE_HANDLER:
    def ref_date(date):
        d = int(re.sub('\D', '', date))
        out_date = datetime.utcfromtimestamp(d / 1000).strftime('%Y-%m-%d')
        return out_date

    def long_date(date):
        d = int(re.sub('\D', '', date))
        out_date = datetime.utcfromtimestamp(
            d / 1000).strftime(
                '%Y-%m-%d %H:%M:%S')
        return out_date
