import re
from ftplib import FTP
from ftp_parser import Parser


class SECFTP:
    def __init__(self, connection):
        self.parser = Parser()
        self.connection = FTP('ftp.sec.gov', 'anonymous')

    def __url_to_path(self, url):
        path = \
            re.sub(r"-|.txt", "", url) + '/' + \
            re.sub('.txt', '.hdr.sgml', url.split("/")[-1])
        return path

    def __download(self, path):
        x = []
        self.connection.retrbinary('RETR ' + path, x.append)
        return ''.join(x)

    def __download_parsed(self, path):
        print('downloading ' + path)
        f = self.__download(path)
        return self.parser.run_header(f)

    def get_deadline(self, url):
        path = self.__url_to_path(url)
        try:
            x = self.__download_parsed(path)['PERIOD'][0]
            prd = x[:4] + '-' + x[4:6] + '-' + x[6:8]
            body = {'period': prd}
            try:
                body['doc_count'] = \
                    int(self.__download_parsed(
                        path)['PUBLIC-DOCUMENT-COUNT'][0])
            except:
                body['doc_count'] = None
        except:
            body = {'period': None}
