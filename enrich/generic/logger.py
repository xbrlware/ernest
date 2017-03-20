#!/usr/bin/env python2.7

import logging


class LOGGER:
    def __init__(self, logger_name, log_file=None):
        self.lname = logger_name
        self.fname = log_file

    def create_parent(self):
        logger = logging.getLogger(self.lname)
        logger.setLevel(logging.DEBUG)

        logging.captureWarnings(True)

        formatter = logging.Formatter(
            '[%(asctime)s] [%(name)s] [%(levelname)s] :: %(message)s')

        if self.fname is not None:
            fh = logging.FileHandler(self.fname)
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger
