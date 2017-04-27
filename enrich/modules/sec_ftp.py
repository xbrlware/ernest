import logging
import re
import requests

from ftp_parser import Parser


class SECFTP:
    def __init__(self, parent_logger):
        self.parser = Parser()
        self.logger = logging.getLogger(parent_logger + '.sec_ftp')
        self.session = requests.Session()

    def __url_to_path(self, url):
        path = \
            re.sub(r"-|.txt", "", url) + '/' + \
            re.sub('.txt', '.hdr.sgml', url.split("/")[-1])
        return 'https://www.sec.gov/Archives/' + path

    def __download(self, path):
        try:
            req = self.session.get(path, timeout=10.0)
        except requests.exceptions.ConnectionError as e:
            self.logger.error('{0}{1}'.format(path, e))
            return ''
        return req.text

    def __download_parsed(self, fp, field):
        try:
            rh = self.parser.run_header(fp)[field][0]
        except KeyError as e:
            self.logger.error('{0}|{1}'.format('KEYERROR', e))
            rh = None
        return rh

    def get_deadline(self, url):
        path = self.__url_to_path(url)
        self.logger.info('{0}|{1}'.format('DOWNLOADING', path))
        f = self.__download(path)
        x = self.__download_parsed(f, 'PERIOD')
        try:
            prd = x[:4] + '-' + x[4:6] + '-' + x[6:8]
        except (TypeError, KeyError) as e:
            self.logger.error('{0}|{1}'.format('PERIOD', e))
            prd = None
        body = {'period': prd}
        try:
            body['doc_count'] = \
                int(self.__download_parsed(f, 'PUBLIC-DOCUMENT-COUNT'))
        except TypeError as e:
            self.logger.error('{0}|{1}'.format('PUBLIC-DOCUMENT-COUNT', e))
            body['doc_count'] = None
        return body
