import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

HOSTNAME = 'localhost'
HOSTPORT = 9205
client = Elasticsearch([
    {'host' : HOSTNAME, 'port' : HOSTPORT}
])


INDEX     = 'ernest_touts'
FORM_TYPE = 'tout'

mapping = {}
mapping[FORM_TYPE] = {
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
    "properties": {
       "date": {
            "type"   : "date",
            "format" : "yyyy-MM-dd HH:mm:ss"
       }
    }
}

client.indices.create(index = INDEX)
client.indices.put_mapping(index     = INDEX, 
                            doc_type = FORM_TYPE,
                            body     = mapping)
