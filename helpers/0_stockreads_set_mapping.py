import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

HOSTNAME = 'localhost'
HOSTPORT = 9205
client = Elasticsearch([
    {'host' : HOSTNAME, 'port' : HOSTPORT}
])

client.indices.create(index='ernest_touts', body={
    "settings" : {
        "number_of_shards"   : 5,
        "number_of_replicas" : 1
    },
    "mappings": {
      "tout": {
            "dynamic_templates": [
               {
                  "string_cat": {
                     "mapping": {
                        "fields": {
                           "{name}": {
                              "type": "string"
                           },
                           "cat": {
                              "index": "not_analyzed",
                              "type": "string"
                           }
                        },
                        "type": "multi_field"
                     },
                     "match": "*",
                     "match_mapping_type": "string"
                  }
               }
            ],
            "properties": {
               "author": {
                  "type": "string",
                  "fields": {
                     "cat": {
                        "type": "string",
                        "index": "not_analyzed"
                     }
                  }
               },
               "content": {
                  "type": "string"
               },
               "date": {
                  "type": "date",
                  "format": "yyyy-MM-dd HH:mm:ss"
               },
               "mentions": {
                  "properties": {
                     "freq": {
                        "type": "long"
                     },
                     "ticker": {
                        "type": "string",
                        "fields": {
                           "cat": {
                              "type": "string",
                              "index": "not_analyzed"
                           }
                        }
                     }
                  }
               },
               "title": {
                  "type": "string",
                  "fields": {
                     "cat": {
                        "type": "string",
                        "index": "not_analyzed"
                     }
                  }
               }
            }
         }
      }
   })