#!/usr/bin/env python2.7

import logging


class KEY_CHECKER:
    def __init__(self, logger_parent):
        self.logger = logging.getLogger(logger_parent + '.key_checker')
        self.return_value = None

    def get_value(self, v, keys):
        for k in keys:
            try:
                tmp = v[k]
            except:
                self.logger.debug('Key not found {}'.format(k))
                tmp = self.return_value
                break
        return tmp
