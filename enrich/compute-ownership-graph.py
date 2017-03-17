'''
    Create ownership index from edgar_index index

    ** Note **
    This runs prospectively using the --last-week parameter
    to grab docs retrospectively for the previous nine days
'''

import re
import json
import argparse

from operator import itemgetter
from itertools import chain, groupby
from datetime import datetime
# import findspark; findspark.init()
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
# from pyspark import SparkContext


class COMPUTE_OWNERSHIP:
    def __init__(self, args):
        with open(args.config_path, 'r') as inf:
            config = json.load(inf)
            self.config = config

        self.query = {
            "_source": [
                "ownershipDocument.periodOfReport",
                "ownershipDocument.reportingOwner",
                "ownershipDocument.issuer",
                "header.ACCESSION-NUMBER",
                "header.ISSUER.COMPANY-DATA.ASSIGNED-SIC"
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "filtered": {
                                "filter": {
                                    "exists": {
                                        "field": "ownershipDocument"}}}}]}}}

        if args.last_week:
            self.query['query']['bool']['must'].append({
                "range": {
                    "ownershipDocument.periodOfReport": {
                        "gte": str(date.today() - timedelta(days=9))}}})
        elif not args.from_scratch:
            raise Exception('must chose either --from-scratch or --last-week')

        self.client = Elasticsearch([{
            'host': config["es"]["host"],
            'port': config["es"]["port"]
        }], timeout=60000)

    def get_id(self, x):

        front = '__'.join(map(lambda x: re.sub(' ', '_', str(x)), x[:9]))
        try:
            id = front + '__' + x[9]['COMPANY-DATA'][0]['ASSIGNED-SIC'][0]
        except:
            id = front + "__0000"

        return (id,) + x

    def clean_logical(self, x):
        tmp = str(x).lower()
        if tmp == 'true':
            return 1
        elif tmp == 'false':
            return 0
        else:
            return x

    def _get_owners(self, r):
        return {
            "isOfficer": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isOfficer', 0)),
            "isTenPercentOwner": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isTenPercentOwner', 0)),
            "isDirector": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isDirector', 0)),
            "isOther": self.clean_logical(
                r.get('reportingOwnerRelationship',
                      {}).get('isOther', 0)),
            "ownerName": self.clean_logical(
                r.get('reportingOwnerId',
                      {}).get('rptOwnerName', 0)),
            "ownerCik": self.clean_logical(
                r.get('reportingOwnerId',
                      {}).get('rptOwnerCik', 0))
        }

    def get_owners(self, val):
        try:
            sic = val['header']['ISSUER'][0]
            ['COMPANY-DATA'][0]
            ['ASSIGNED-SIC'][0]
        except (KeyError, IndexError):
            sic = None
        top_level_fields = {
            "issuerCik": val['ownershipDocument']['issuer']['issuerCik'],
            "issuerName": val['ownershipDocument']['issuer']['issuerName'],
            "issuerTradingSymbol": val['ownershipDocument']['issuer']
            ['issuerTradingSymbol'],

            "periodOfFiling": val['ownershipDocument']['periodOfReport'],
            "sic": sic
        }
        ro = val['ownershipDocument']['reportingOwner']
        ro = [ro] if isinstance(ro, dict) else ro
        ros = map(self._get_owners, ro)
        for r in ros:
            r.update(top_level_fields)
        return ros

    def get_properties(self, x):
        tmp = {
            "issuerCik": str(x['issuerCik']).zfill(10),
            "issuerName": str(x['issuerName']).upper(),
            "issuerTradingSymbol": str(x['issuerTradingSymbol']).upper(),
            "ownerName": str(x['ownerName']).upper(),
            "ownerCik": str(x['ownerCik']).zfill(10),
            "isDirector": int(x['isDirector']),
            "isOfficer": int(x['isOfficer']),
            "isOther": int(x['isOther']),
            "isTenPercentOwner": int(x['isTenPercentOwner']),
            "periodOfFiling": str(x['periodOfFiling']),
            "sic": x['sic'],
            "min_date": x['periodOfFiling'],
            "max_date": x['periodOfFiling']
        }
        return (
            (tmp['issuerCik'],
             tmp['issuerName'],
             tmp['issuerTradingSymbol'],
             tmp['ownerName'],
             tmp['ownerCik'],
             tmp['isDirector'],
             tmp['isOfficer'],
             tmp['isOther'],
             tmp['isTenPercentOwner'],
             tmp['sic'],
             tmp['min_date'],
             tmp['max_date']),
            tmp['periodOfFiling']
        )

    def coerce_out(self, x):
        tmp = {
            "issuerCik": str(x[0][0]),
            "issuerName": str(x[0][1]),
            "issuerTradingSymbol": str(x[0][2]),
            "ownerName": str(x[0][3]),
            "ownerCik": str(x[0][4]),
            "isDirector": int(x[0][5]),
            "isOfficer": int(x[0][6]),
            "isOther": int(x[0][7]),
            "isTenPercentOwner": int(x[0][8]),
            "sic": x[0][9],
            "min_date": str(x[1]['min_date']),
            "max_date": str(x[1]['max_date'])
        }
        tmp['id'] = str(tmp['issuerCik']) \
            + '__' \
            + str(re.sub(' ', '_', tmp['ownerName'])) \
            + '__' \
            + str(tmp['ownerCik']) \
            + '__' \
            + str(tmp['isDirector']) \
            + '__' \
            + str(tmp['isOfficer']) \
            + '__' \
            + str(tmp['isOther']) \
            + '__' \
            + str(tmp['isTenPercentOwner']) \
            + '__' \
            + str(tmp['sic'])
        return ('-', tmp)

    def find_max_date(self, l):
        max_date = datetime.strptime(l[0][11], "%Y-%m-%d")
        for ele in l[1:]:
            t = datetime.strptime(l[0][11], "%Y-%m-%d")
            if t > max_date:
                max_date = t
        return l[0][:11] + (max_date.strftime("%Y-%m-%d"),)

    def make_list(self, g):
        z = [x for x, y in g]
        return self.find_max_date(z)

    def main(self):
        df = list(chain.from_iterable(
            [self.get_owners(a['_source']) for a in scan(
                self.client,
                index=self.config['forms']['index'],
                doc_type=self.config['forms']['_type'],
                query=self.query)]))

        df2 = [self.get_properties(d) for d in df]

        df2.sort(key=itemgetter(0))

        df3 = [self.make_list(g) for k, g in groupby(df2, key=itemgetter(0))]

        if args.last_week:
            df4 = [self.get_id(d) for d in df3]
            print(df4)
            """
            min_dates = {}
            for i in ids:
                try:
                    mtc = self.client.get(
                        index=self.config['ownership']['index'],
                        doc_type=self.config['ownership']['_type'],
                        id=i)
                    min_dates[i] = mtc['_source']['min_date']
                except:
                    print('missing \t %s' % i)

        elif args.from_scratch:
            df_out = df_range

        map(self.coerce_out, df3).saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf={
                "es.nodes": self.config['es']['host'],
                "es.port": str(self.config['es']['port']),
                "es.resource": '%s/%s' % (self.config['ownership']['index'],
                                          self.config['ownership']['_type']),
                "es.mapping.id": 'id',
                "es.write.operation": "upsert"
            }
        )
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='grab_new_filings')
    parser.add_argument('--from-scratch',
                        dest='from_scratch',
                        action="store_true")
    parser.add_argument('--last-week', dest='last_week', action="store_true")
    parser.add_argument("--config-path",
                        type=str,
                        action='store',
                        default='../config.json')
    args = parser.parse_args()

    cog = COMPUTE_OWNERSHIP(args)
    cog.main()
