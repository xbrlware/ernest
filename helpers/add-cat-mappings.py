import json
import argparse
from elasticsearch import Elasticsearch

parser = argparse.ArgumentParser()
parser.add_argument("--index", type=str, action='store', required=True)
parser.add_argument("--n-shards", type=int, action='store', default=5)
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
parser.add_argument("--no-cat", type=str, action='store', default='')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([{
    "host" : config['es']['host'],
    "port" : config['es']['port'],   
}], timeout = 6000)

if args.no_cat:
    whitelist = dict([(nc, {"type" : "string"}) for nc in args.no_cat.split(',')])
else:
    whitelist = {}

dynamic_templates = { 
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
    ],
    "properties" : whitelist
}

client.indices.create(index=config[args.index]['index'], body={
    "settings" : {
        "number_of_shards"   : args.n_shards,
        "number_of_replicas" : 1
    },
    "mappings" : {
        config[args.index]['_type'] : dynamic_templates
    } 
})
