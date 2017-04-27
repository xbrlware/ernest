#!/usr/bin/env python2.7

import re
import logging

from datetime import datetime


class DATE_HANDLER:
    def __init__(self, parent):
        self.logger = logging.getLogger(parent + '.date_handler')

    def fmt_date(self, date_string, func_name):
        try:
            if date_string is None:
                out_date = date_string
            else:
                if func_name == 'ref_date':
                    out_date = datetime.utcfromtimestamp(
                        date_string / 1000).strftime('%Y-%m-%d')
                elif func_name == 'long_date':
                    out_date = datetime.utcfromtimestamp(
                        date_string / 1000).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    out_date = None
            return out_date
        except ValueError:
            # self.logger.debug(
            #    '[ValueError]|{0}|{1}'.format(func_name, date_string))
            return None
        except:
            self.logger.debug(
                '[Exception]|{0}|{1}'.format(func_name, date_string))
            return None

    def scrub_date_string(self, date_string, func_name):
        try:
            d = int(re.search('/Date\((.+)\)/', date_string).group(1))
            return self.fmt_date(d, func_name)
        except:
            # self.logger.debug(
            #    '[Exception]|{0}|{1}'.format(
            #         func_name, date_string))
            return None

    def ref_date(self, date):
        # d = int(re.sub('\D', '', date))
        return self.scrub_date_string(date, 'ref_date')

    def long_date(self, date):
        return self.scrub_date_string(date, 'long_date')
