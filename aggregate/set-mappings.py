#!/usr/bin/env python

"""
    set-mappings.py
    Set `*_stringified` fields to `not_analyzed`
"""

import json
import argparse
from elasticsearch import Elasticsearch


parser = argparse.ArgumentParser(description='financials')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{
    "host": config['es']['host'],
    "port": config['es']['port'],
}], timeout=6000)

dynamic_templates = {
    "dynamic_templates": [
        {
            "string_cat": {
                "mapping": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "match": "*_stringified",
                "match_mapping_type": "string"
            }
        }
    ]
}

try:
    client.indices.create(index=config['agg']['index'], body={
        "mappings": {
            config['agg']['_type']: dynamic_templates
        }
    })
except:
    client.indices.put_mapping(
        index=config['agg']['index'],
        doc_type=config['agg']['_type'],
        body={config['agg']['_type']: dynamic_templates}
    )
