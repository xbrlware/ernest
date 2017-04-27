import datetime
import json
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from query_json.normalize_financials_queries import nfq_from_scratch
from query_json.normalize_financials_queries import nfq_most_recent
from query_json.normalize_financials_queries import normalize_query


class ENRICH_NORMALIZE_FINANCIALS:
    def __init__(self, args, parent_logger):
        with open(args.config_path, 'rb') as inf:
            config = json.load(inf)
            self.config = config

        self.args = args
        self.logger = logging.getLogger(parent_logger + '.enrich')
        self.client = Elasticsearch([{
            'host': config['es']['host'],
            'port': config['es']['port']}
            ])

        if args.from_scratch:
            self.query = nfq_from_scratch()
        elif args.most_recent:
            self.query = nfq_most_recent()

    def to_datetime(self, x):
        r = map(int, x.split('-'))
        date = datetime.date(r[0], r[1], r[2])
        return date

    def normalize(self, x, pp):
        query = normalize_query(x['cik'], pp)
        ref = []
        for i in scan(self.client,
                      index=self.config['aq_forms_enrich']['index'],
                      query=query):
            ref.append(i)
        if len(ref) == 1:
            ref = ref[0]
            docvals = x['__meta__']['financials']
            refvals = ref['_source']['__meta__']['financials']
            for k, v in docvals.iteritems():
                if type(v) == dict:
                    tag = str(k)
                    try:
                        if docvals[tag]['from'] is not None:
                            try:
                                docvals[tag][u'norm'] = \
                                    docvals[tag]['value'] - \
                                    refvals[tag]['value']
                                docvals[tag][u'duration'] = \
                                    (self.to_datetime(docvals[tag]['to']) -
                                        self.to_datetime(
                                            docvals[tag]['from'])).days / 30
                            except:
                                docvals[tag][u'norm'] = None
                    except:
                        self.logger.warn('interpolated value')
                else:
                    pass
        else:
            self.logger.warn('duplicate period assignment')
        x['__meta__']['financials']['scaled'] = True
        return x

    def get_period_ref(self, fp):
        lkp = {'Q2': 'Q1', 'Q3': 'Q2', 'Q4': 'Q3'}
        if 'Q1' in fp:
            pass
        else:
            c = fp.split('__')
            pr = lkp[c[0]]
            pid = pr + '__' + c[1]
            return pid

    def enrich(self):
        for doc in scan(self.client,
                        index=self.config['aq_forms_enrich']['index'],
                        query=self.query):
            fp = doc['_source']['sub']['fpID']
            try:
                pp = self.get_period_ref(fp)
            except KeyError:
                self.logger.warn('non-standard period id')
            if pp is not None:
                out = self.normalize(doc['_source'], pp)
            else:
                out = doc['_source']
            res = self.client.index(
                index=self.config['aq_forms_enrich']['index'],
                doc_type=self.config['aq_forms_enrich']['_type'],
                id=doc["_id"],
                body=out
            )
            self.logger.info(res)
