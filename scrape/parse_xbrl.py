#!/usr/bin/env python2.7

import argparse
import json
import logging

from os import listdir
from os.path import isfile, join
from generic.zip_directory_handler import ZIP_DIRECTORY_HANDLER
from generic.logger import LOGGER
from xbrl import XBRLParser, GAAPSerializer


class PARSE_XBRL:
    def __init__(self, args, parent_logger):
        self.args = args
        self.logger = logging.getLogger(parent_logger + '.parse_xbrl')
        self.zip_handler = ZIP_DIRECTORY_HANDLER()
        self.xbrl_parser = XBRLParser()
        self.serializer = GAAPSerializer()

        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

    def __file_list(self, directory):
        return [f for f in listdir(directory) if isfile(join(directory, f))]

    def __get_xbrl_content(self, file_name):
        with open(file_name, 'r') as inf:
            try:
                content = self.xbrl_parser.parse(inf)
                return content
            except:
                pass
                return False

    def parse_xbrl(self, zip_directory, unzip_directory):
        self.zip_handler.unzip_directory(zip_directory, unzip_directory)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='parse xbrl')
    parser.add_argument("--config-path",
                        type=str, action='store', default='../config.json')
    parser.add_argument('--log-file',
                        type=str,
                        dest='log_file',
                        action='store',
                        required=True)
    args = parser.parse_args()
    logger = LOGGER('xbrl_download', args.log_file).create_parent()

    px = PARSE_XBRL(args, 'parse_xbrl')
    px.parse_xbrl('/home/ubuntu/sec/filings__2017__03',
                  '/home/ubuntu/sec/unzipped__2017__03')
