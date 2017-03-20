#!/usr/bin/env python2.7

'''
    Update ernest_forms_cat index with newly ingested edgar_index documents

    ** Note **
    This runs prospectively using a back-fill parameter only to grab docs
    that have not been tried or that have failed

'''

import argparse
import logging
import json
import re
import requests
import time
import xmltodict

from datetime import date, datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, parallel_bulk


class EDGAR_INDEX_FORMS:
    def __init__(self, args):
        self.logger = logging.getLogger('scrape_edgar.edgar_index_forms')
        self.T = time.time()
        headers = requests.utils.default_headers()
        self.headers = headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) \
            AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/56.0.292476 \
            Chrome/56.0.2924.76 Safari/537.36"
        })
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config
            self.client = Elasticsearch([{'host': config['es']['host'],
                                          'port': config['es']['port']}])
            self.forms_index = config['forms']['index']
            self.edgar_index = config['edgar_index']['index']

        self.params = {'back_fill': args.back_fill,
                       'start_date': datetime.strptime(
                           args.start_date, '%Y-%m-%d'),
                       'end_date': datetime.strptime(
                           args.end_date, '%Y-%m-%d'),
                       'form_types': map(int, args.form_types.split(',')),
                       'section': args.section}
        self.docs = args.section in ['body', 'both']
        self.header = args.section in ['header', 'both']
        if (not self.docs) and (not self.header):
            raise Exception('section must be in [body, header, both]')

        # Must be the right form type and between the dates
        must = [{"terms": {"form.cat": self.params['form_types']}},
                {"range": {
                    "date": {
                        "gte": self.params['start_date'],
                        "lte": self.params['end_date']
                    }
                }}]

        # If not back filling, only load forms that haven't been tried
        if not args.back_fill:
            must.append({"filtered": {
                "filter": {
                    "or": [
                        {"missing": {"field": "download_try2"}},
                        {"missing": {"field": "download_try_hdr"}},
                    ]
                }
            }})

        # Otherwise, try forms that haven't been tried or have failed
        else:
            must.append({"bool": {
                "should": [
                    {
                        "filtered": {
                            "filter": {
                                "or": [
                                    {"missing": {"field": "download_try2"}},
                                    {"missing": {"field": "download_try_hdr"}}
                                ]
                            }
                        }
                    },
                    {
                        "bool": {
                            "must": [
                                {"match": {"download_success2": False}},
                                {"range": {"try_count_body": {"lte": 6}}}
                            ]
                        }
                    },
                    {
                        "bool": {
                            "must": [
                                {"match": {"download_success_hdr": False}},
                                {"range": {"try_count_hdr": {"lte": 6}}}
                            ]
                        }
                    }
                ],
                "minimum_should_match": 1
            }})

        self.query = {
            "query": {
                "bool": {
                    "must": must
                }
            }
        }

    def url_to_path(self, url, file_type):
        base = 'https://www.sec.gov/Archives/edgar/data/'
        url = url.split("/")
        n_sub = re.sub('\D', '', url[-1])
        if file_type == 'hdr':
            type_sub = re.sub('.txt', '.hdr.sgml', url[-1])
        else:
            type_sub = url[-1]

        return base + url[2] + "/" + n_sub + "/" + type_sub

    def download_requests(self, path):
        try:
            r = requests.get(path, headers=self.headers,
                             verify=False, timeout=1)
        except requests.exceptions.ConnectionError:
            self.logger.debug('[ConnectionError] :: {}'.format(path))
            r = None
        except requests.exceptions.Timeout:
            self.logger.debug('[TimeoutError] :: {}'.format(path))
            r = 1
        return r

    def download(self, path):
        x = ''
        r = self.download_requests(path)
        if r is None:
            t = 0
            while t < 10:
                time.sleep(10)
                r = self.download_requests(path)
                if r is None:
                    t += 1
                else:
                    break

        self.logger.info('[status: {0}]: {1}'.format(r.status_code, path))
        if r.status_code == requests.codes.ok:
            try:
                x = ''.join([i for i in r.text])
            except:
                self.logger.debug('Unable to parse: {}'.format(path))
        return x

    def download_parsed(self, path):
        return self.run_header(self.download(path))

    def run_header(self, txt):
        hd1 = ''
        if len(txt) > 0:
            try:
                txt = re.sub('\r', '', txt)
                hd0 = txt[txt.find('<ACCESSION-NUMBER>'):txt.find('<DOCUMENT>')]
                hd1 = filter(None, hd0.split('\n'))
            except:
                self.logger.debug('Error :: run_header :: %s' % (txt))

        return self.parse_header(hd1)

    def parse_header(self, hd):
        curr = {}
        i = 0
        while i < len(hd):
            h = hd[i]
            if re.search('>.+', h) is not None:
                # Don't descend
                key = re.sub('<|(>.*)', '', h)
                val = re.sub('<[^>]*>', '', h)
                curr[key] = [val]
                i += 1
            else:
                if re.search('/', h) is None:
                    key = re.sub('<|(>.*)', '', h)
                    end = filter(lambda i: re.search('</' + h[1:], hd[i]),
                                 range(i, len(hd)))
                    tmp = curr.get(key, [])
                    if len(end) > 0:
                        curr[key] = tmp + [self.parse_header(
                            hd[(i + 1):(end[0])])]
                        i = end[0]
                    else:
                        curr[key] = tmp + [None]
                        i = i + 1
                else:
                    i = i + 1
        return curr

    def get_headers(self, a):
        path = self.url_to_path(a['_id'], 'hdr')
        out = {"_id": a['_id'],
               "_type": a['_type'],
               "_index": self.forms_index,
               "_op_type": 'update',
               "doc_as_upsert": True
               }
        out_log = {"_id": a['_id'],
                   "_type": a['_type'],
                   "_index": self.edgar_index,
                   "_op_type": "update"
                   }
        try:
            out['doc'] = {"header": self.download_parsed(path)}
            out_log['doc'] = {"download_try_hdr": True,
                              "download_success_hdr": True}
            return out, out_log
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            try:
                x = a['_source']['try_count_hdr']
            except:
                x = 0
                out_log['doc'] = {"download_try_hdr": True,
                                  "download_success_hdr": False,
                                  "try_count_hdr": x + 1}
                self.logger.info('failed @ {}'.format(path))
                return None, out_log

    def get_docs(self, a):
        path = self.url_to_path(a['_id'], 'doc')
        out = {"_id": a['_id'],
               "_type": a['_type'],
               "_index": self.forms_index,
               "_op_type": "update",
               "doc_as_upsert": True
               }
        out_log = {"_id": a['_id'],
                   "_type": a['_type'],
                   "_index": self.edgar_index,
                   "_op_type": "update"
                   }
        try:
            page = self.download(path)
            if page:
                s_string = 'ownershipDocument>'
                page = '<' + s_string + page.split(s_string)[1] + s_string
                page = re.sub('\n', '', page)
                page = re.sub(
                    '>([0-9]{4}-[0-9]{2}-[0-9]{2})-[0-9]{2}:[0-9]{2}<', '>\\1<',
                    page)
                page = re.sub('([0-9]{2})(- -)([0-9]{2})', '\\1-\\3', page)
                parsed_page = xmltodict.parse(page)
                out['doc'] = parsed_page
                out_log['doc'] = {"download_try2": True,
                                  "download_success2": True}
            else:
                out = None
                out_log['doc'] = {"download_try2": True,
                                  "download_success2": False,
                                  "try_count_body": 1}
                self.logger.debug('failed @ %s' % (path))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            try:
                x = a['_source']['try_count_body']
                self.logger.info('found try count body')
            except:
                x = 0

            out = None
            out_log['doc'] = {"download_try2": True,
                              "download_success2": False,
                              "try_count_body": x + 1}
            self.logger.debug('failed @ %s' % (path))

        return out, out_log

    def chunk(self, chunk, docs, header):
        self.logger.info('[chunk]: begin processing {} docs'.format(len(chunk)))
        for a in chunk:
            if docs:
                out, out_log = self.get_docs(a)
                if out:
                    yield out
                yield out_log
            if header:
                out, out_log = self.get_headers(a)
                if out:
                    yield out
                yield out_log
            time.sleep(2)

        self.logger.info('[chunk]: done processing docs')

    def load_chunk(self, c, docs, header):
        for a, b in parallel_bulk(self.client,
                                  [x for x in self.chunk(c, docs, header)]):
            if a is not True:
                self.logger.info(b)

    def main(self):
        chunk_size = 200
        cntr = 0
        chunk = []

        resp = self.client.count(index=self.config['forms']['index'])
        count_in = resp['count'] or None

        for a in scan(self.client, index=self.edgar_index,
                      query=self.query, size=chunk_size):
            chunk.append(a)
            if len(chunk) >= chunk_size:
                cntr += len(chunk)
                self.load_chunk(chunk, self.docs, self.header)
                self.logger.info('%d docs in %f' % (cntr, time.time() - self.T))
                chunk = []

        self.load_chunk(chunk, self.docs, self.header)

        resp = self.client.count(index=self.config['forms']['index'])
        count_out = resp['count'] or None

        self.logger.info('%d in, %d out' % (count_in, count_out))

        return [count_in, count_out]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='scrape-edgar-forms')
    parser.add_argument("--back-fill", action='store_true')
    parser.add_argument("--start-date", type=str, action='store', required=True)
    parser.add_argument("--end-date",
                        type=str,
                        action='store',
                        default=date.today().strftime('%Y-%m-%d'))
    parser.add_argument("--form-types", type=str, action='store', required=True)
    parser.add_argument("--section", type=str, action='store', default='both')
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    args = parser.parse_args()

    eif = EDGAR_INDEX_FORMS(args)
    eif.main()
