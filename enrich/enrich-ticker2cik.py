import json
import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

# --
# cli 

parser = argparse.ArgumentParser()
parser.add_argument("--config-path",   type = str, action = 'store', default='../config.json')
parser.add_argument("--index", type=str, action='store', required=True)
parser.add_argument("--field-name", type=str, action='store', required=True)
args = parser.parse_args()

config = json.load(open(args.config_path))
# config = json.load(open('../config.json'))

client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']
}], timeout = 60000)


# --
# Run

def get_lookup():
    query = {"_source" : ["max_date", "sic", "cik", "ticker"]}
    out = {}
    for a in scan(client, index=config['symbology']['index'], query=query):
        out[a['_source']['ticker']] = a['_source']
    
    return out

def run(lookup): 
    query = {
        "field" : args.field_name,
        "query" : {
            "filtered" : {
                "filter" : {
                    "and" : [
                        {
                            "missing" : {
                                "field" : "__meta__.sym.match_attempted"
                            }                    
                        },
                        {
                            "exists" : {
                                "field" : args.field_name
                            }
                        }
                    ]
                }
            }
        }
    }

    for a in scan(client, index=config[args.index]['index'], query=query): 
        sym = {"match_attempted" : True}
        mtc = lookup.get(a['fields'][args.field_name], {})
        sym.extend(mtc)
        yield {
            "_id"      : a['_id'],
            "_type"    : a['_type'],
            "_index"   : a['_index'],
            "_op_type" : "update",
            "doc" : {
                "__meta__" : {
                    "sym" : sym
                }
            }
        }


if __name__ == "__main__":
    lookup = get_lookup()
    for a,b in streaming_bulk(client, run(lookup), chunk_size=1000, raise_on_error=False):
        print a, b