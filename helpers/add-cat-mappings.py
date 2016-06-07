import json
import argparse
from elasticsearch import Elasticsearch

parser = argparse.ArgumentParser()
parser.add_argument("--index", type=str, action='store', required=True)
parser.add_argument("--doc-types", type=str, action='store', required=True)
parser.add_argument("--n-shards", type=int, action='store', default=5)
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{
    "host" : config['es']['host'],
    "port" : config['es']['port'],   
}], timeout = 6000)

def dynamic_templates():
    return { 
        "dynamic_templates": [
            {
                "string_cat": {
                    "mapping": {
                        "type": "multi_field",
                        "fields": {
                            "{name}": {
                                "type": "string"
                            },
                            "cat": {
                                "type": "string",
                                "index": "not_analyzed"
                            }
                        }
                    },
                    "match": "*",
                    "match_mapping_type": "string"
                }
            }
        ]
    }

mappings = dict([(doc_type, dynamic_templates()) for doc_type in args.doc_types.split(',')])

client.indices.create(index=args.index, body={
    "settings" : {
        "number_of_shards"   : args.n_shards,
        "number_of_replicas" : 0
    },
    "mappings" : mappings 
})