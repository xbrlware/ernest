import argparse

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from elasticsearch.helpers import reindex


# -- 
# cli

parser = argparse.ArgumentParser()
parser.add_argument("--n-shards", type=int, action='store')
parser.add_argument("--config-dict", type=str, action='store')
parser.add_argument("--target-index", type=str, action='store')
parser.add_argument("--config-path", type = str, action = 'store', default='../config.json')
args = parser.parse_args()


# -- 
# config

config_path = args.config_path
config      = json.load(open(config_path))


# -- 
# es connection

client = Elasticsearch([{
    "host" : config['es']['host'],
    "port" : config['es']['port'],   
}], timeout = 6000)


# --
# define params

SOURCE_INDEX = config[args.config_dict]['index']
DOC_TYPE     = config[args.config_dict]['_type']
TARGET_INDEX = args.target_index


# -- 
# build

client.indices.create(index=TARGET_INDEX, body={
    "settings" : {
        "number_of_shards"   : args.n_shards,
        "number_of_replicas" : 0
    },
    "mappings" : { 
      DOC_TYPE : { 
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
      }
})


reindex(client, SOURCE_INDEX, TARGET_INDEX, chunk_size=5000)