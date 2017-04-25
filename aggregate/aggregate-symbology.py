"""
    Compute instances of ticker, name and SIC changes for all CIKS
"""

import json
import argparse
import itertools
from collections import OrderedDict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan


parser = argparse.ArgumentParser(description='aggregate-symbology')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch(host=config['es']['host'],
                       port=config['es']['port'])
i = config['symbology']['index']
d = config['symbology']['_type']


def drop_empties(records):
    # Remove index entries that have the same name as the previous record
    i = 1
    while i < len(records):
        if (records[i]['__meta__']['source'] == 'index') and \
                (records[i]['name'] == records[i-1]['name']):
            del records[i]
        else:
            i += 1
    # Remove index enties that have the same name as the next record
    i = 0
    while i < (len(records) - 1):
        if (records[i]['__meta__']['source'] == 'index') and \
                (records[i]['name'] == records[i+1]['name']):
            del records[i]
        else:
            i += 1
    return records


def make_current_symbology(records):
        current_symbology = {
            'name': records[-1]['name'],
            'ticker': None,
            'sic': None,
            'sic_lab': None
        }

        for record in records:

            if record['ticker']:
                current_symbology['ticker'] = record['ticker']

            if record['sic']:
                current_symbology['sic'] = record['sic']

            if record['__meta__']:
                if record['__meta__']['sic_lab']:
                    current_symbology['sic_lab'] = \
                            record['__meta__']['sic_lab']

        return current_symbology


def _changes(records, field):
    """ Determines changes in name, SIC and symbol """
    old_record = records[0]
    for new_record in records[1:]:
        if new_record[field] and old_record[field] and \
                (new_record[field] != old_record[field]):
            yield OrderedDict([
                ("field", field),
                ("old_val", old_record[field]),
                ("new_val", new_record[field]),
                ("old_date", old_record['max_date']),
                ("new_date", new_record['min_date']),
            ])
        old_record = new_record


def all_changes(records):
    """ Determines changes in name, SIC and symbol """
    records = drop_empties(sorted(records, key=lambda x: x['min_date']))
    historic = list(
        itertools.chain(
            *[_changes(records, field) for field in ['name', 'sic', 'ticker']]
            )
        )

    return {
        "symbology": sorted(historic, key=lambda x: x['new_date']),
        "current_symbology": make_current_symbology(records)
    }


all_changes(
    [
        (doc['_source']['cik'],
         doc['_source']) for doc in scan(client, index=i, doc_type=d)
    ])


"""
rdd.map(lambda x: (x[1]['cik'], x[1]))\
    .groupByKey()\
    .mapValues(all_changes)\
    .map(
        lambda x: (x[0], x[1]['current_symbology'], tuple(x[1]['symbology'])))\
    .map(lambda x: ('-', {
        "cik": str(x[0]).zfill(10),
        "current_symbology": x[1],
        "symbology": x[2],
        "symbology_stringified": tuple(
            map(json.dumps, x[2])) if len(x[2]) > 0 else None,
    }))\
    .mapValues(json.dumps)\
    .saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass='org.apache.hadoop.io.NullWritable',
        valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
        conf={
            'es.input.json': 'true',
            'es.nodes': config['es']['host'],
            'es.port': str(config['es']['port']),
            'es.resource': '%s/%s' % (
                config['agg']['index'], config['agg']['_type']),
            'es.mapping.id': 'cik',
            'es.write.operation': 'upsert'
        }
    )
"""
