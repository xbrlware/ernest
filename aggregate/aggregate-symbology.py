"""
    Compute instances of ticker, name and SIC changes for all CIKS
"""

import json
import argparse
import itertools
from collections import OrderedDict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk


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
            keys = record.keys()
            if 'ticker' in keys:
                current_symbology['ticker'] = record['ticker']

            if 'sic' in keys:
                current_symbology['sic'] = record['sic']

            if '__meta__' in keys:
                if 'sic_lab' in record['__meta__'].keys():
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


def group_by(r):
    return [(key, [v[1] for v in values])
            for key, values in itertools.groupby(r, lambda x: x[0])]


def all_changes(cik, records):
    """ Determines changes in name, SIC and symbol """
    r = drop_empties(sorted(records, key=lambda x: x['min_date']))
    historic = list(
        itertools.chain(
            *[_changes(r, field) for field in ['name', 'sic', 'ticker']]
            )
        )
    sym = sorted(historic, key=lambda x: x['new_date'])
    rv = {
        "cik": cik,
        "symbology": sym,
        "current_symbology": make_current_symbology(r),
    }
    if len(rv['symbology']) > 0:
        rv['symbology_stringified'] = map(json.dumps, sym)
    else:
        rv['symbology_stringified'] = None

    return {
        "_op_type": "update",
        "_index": config['agg']['index'],
        "_type": config['agg']['_type'],
        "_id": cik,
        "doc": rv,
        "doc_as_upsert": True
    }


gp = {}
for doc in scan(client, index=i, doc_type=d):
    try:
        gp[doc['_source']['cik']].append(doc['_source'])
    except KeyError:
        try:
            gp[doc['_source']['cik']] = [doc['_source']]
        except KeyError as e:
            print(e)

for a in streaming_bulk(client,
                        actions=[all_changes(key, gp[key]) for key in gp]):
    print(a)
