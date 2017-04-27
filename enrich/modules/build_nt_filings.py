import json
import logging
import time
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from modules.sec_ftp import SECFTP

from query_json.nt_filings_queries import build_nt_from_scratch
from query_json.nt_filings_queries import build_nt_most_recent
from query_json.nt_filings_queries import enrich_nt_query
from query_json.nt_filings_queries import add_nt_most_recent
from query_json.nt_filings_queries import add_nt_from_scratch
from query_json.nt_filings_queries import match_nt_query


class BUILD_NT_FILINGS:
    def __init__(self, args, parent_logger):
        with open(args.config_path, 'rb') as inf:
            config = json.load(inf)
            self.config = config

        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']
        }], timeout=60000)
        self.args = args
        self.logger_name = parent_logger + '.build_nt_filings'
        self.logger = logging.getLogger(self.logger_name)
        if args.from_scratch:
            self.b_query = build_nt_from_scratch()
            self.t_query = add_nt_from_scratch()
        elif args.most_recent:
            self.b_query = build_nt_most_recent()
            self.t_query = add_nt_most_recent()

    def __enrich_deadline(self, src):
        time.sleep(1)
        src['_enrich'] = SECFTP(self.logger_name).get_deadline(src['url'])
        return src

    def __match_nt(self, doc):
        q = match_nt_query(doc['form'],
                           doc['cik'],
                           doc['_enrich']['period'])
        hits = []
        try:
            for a in scan(self.client,
                          index=self.config['aq_forms_enrich']['index'], query=q):
                hits.append(a)
        except:
            pass

        if len(hits) == 0:
            hit = None
        elif len(hits) > 0:
            hit = list(sorted(hits, key=lambda k: k['_source']['date']))[0]
            hit['_source']['_enrich']['NT_exists'] = True
        return hit

    def __build_nt_filings(self):
        for a in scan(self.client,
                      index=self.config['edgar_index']['index'],
                      query=self.b_query):
            try:
                a['_source']['__meta__']['migrated'] = True
            except KeyError:
                a['_source']['__meta__'] = {'migrated': True}
            self.client.index(
                index=self.config['nt_filings']['index'],
                doc_type=self.config['nt_filings']['_type'],
                id=a['_id'],
                body=a['_source']
            )

    def __enrich_nt_filings_period(self):
        q = enrich_nt_query()
        for doc in scan(self.client,
                        index=self.config['nt_filings']['index'],
                        query=q):
            res = self.client.index(
                index=self.config['nt_filings']['index'],
                doc_type=self.config['nt_filings']['_type'],
                id=doc["_id"],
                body=self.__enrich_deadline(doc['_source'])
                )
            self.logger.info(res)

    def __add_nt_filings_tag(self):
        for a in scan(self.client,
                      index=self.config['nt_filings']['index'],
                      query=self.t_query):
            hit = self.__match_nt(a['_source'])
            a['_source']['__meta__'] = {'match_attempted': True}
            if hit is not None:
                a['_source']['__meta__']['matched'] = True
                res1 = self.client.index(
                    index=self.config['aq_forms_enrich']['index'],
                    doc_type=self.config['aq_forms_enrich']['_type'],
                    id=hit["_id"],
                    body=hit['_source']
                )
                self.logger.info(res1)
                res2 = self.client.index(
                    index=self.config['nt_filings']['index'],
                    doc_type=self.config['nt_filings']['_type'],
                    id=a["_id"],
                    body=a['_source']
                )
                self.logger.info(res2)
            elif hit is None:
                a['_source']['__meta__']['matched'] = False
                res = self.client.index(
                    index=self.config['nt_filings']['index'],
                    doc_type=self.config['nt_filings']['_type'],
                    id=a["_id"],
                    body=a['_source']
                )
                self.logger.info(res)

    def main(self):
        self.__build_nt_filings()
        self.__enrich_nt_filings_period()
        self.__add_nt_filings_tag()
