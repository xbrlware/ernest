'''
    Copy index while increasing the number of shards
    (I feel like this is useful for Spark ETL)
'''

import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import reindex

config = json.load(open('../config.json'))

client = Elasticsearch([{
    "host" : config['es']['host'],
    "port" : config['es']['port'],   
}])


old_index = config[sys.argv[1]]['index']
doc_type  = config[sys.argv[1]]['_type']
new_index = old_index + '_reshard'

# --

client.indices.create(index=new_index, body={
    "settings" : {
        "number_of_shards"   : 20,
        "number_of_replicas" : 0
    }
})

reindex(client, index, new_index, chunk_size=1000)