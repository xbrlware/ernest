import re
import json
import argparse
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from ftplib import FTP
import re
import json
import argparse
from hashlib import sha1
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from pyspark import SparkContext

sc = SparkContext(appName='fye_aggregation')
# -- 
# CLI

parser = argparse.ArgumentParser(description='fye_aggregation')
parser.add_argument('--from-scratch', dest='from_scratch', action="store_true")
parser.add_argument('--most-recent', dest='most_recent', action="store_true")
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()


# --
# config

config = json.load(open('/home/ubuntu/ernest/config.json'))
client = Elasticsearch([{
    'host' : config['es']['host'], 
    'port' : config['es']['port']}
])

if args.from_scratch: 
    query = { 
        "query" : { 
            "filtered" : { 
                "filter" : { 
                    "exists" : { 
                        "field" : "_enrich.period"
                    }
                }
            }
        }
    }
elif args.most_recent: 
    query = { 
        "query" : { 
            "bool" : { 
                "must" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "exists" : { 
                                    "field" : "_enrich.period"
                                }
                            } 
                        }
                    },
                    {
                        "range" : { 
                            "date" : { 
                                "gte" : str(date.today() - timedelta(days=9))
                            }
                        }
                    }
                ]
            }
        }
    }


# __ spark

rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={
        "es.nodes"    : config['es']['host'],
        "es.port"     : str(config['es']['port']),
        "es.resource" : "%s/%s" % (config['aq_forms_enrich']['index'], config['aq_forms_enrich']['_type']),
        "es.query"    : json.dumps(query)
   }
)

# -- 
# functions

def buildSchedule(x):
    key = x[1]['_enrich']['period'][5:]
    cik = x[1]['cik'] #.zfill(10)
    val = x[1]['date']
    return ((key, cik), (val, val)) 


def get_id(x): 
    return x[0][1] + '_' + x[0][0]


def merge_dates(x, min_dates):
    id_ = get_id(x)
    if min_dates.get(id_, False):
        x[1]['min_date'] = min_dates[id_]
    
    return x


def coerce_out(x): 
    fye = x[0][0].split('-')
    return ('-', { 
        'id'       : x[0][1] + '_' + x[0][0],
        'cik'      : x[0][1],
        'fYEMonth' : fye[0],
        'fYEDay'   : fye[1],
        'max_date' : x[1]['max_date'],
        'min_date' : x[1]['min_date']
    })



# -- 
# build base rdd

df_k   = rdd.filter(lambda x: x[1]['form'] == '10-K').map(buildSchedule)\
       .reduceByKey(lambda a,b: (min(a[0], b[0]), max(a[1], b[1])))\
       .mapValues(lambda x: {
        "min_date" : x[0],
        "max_date" : x[1]
    })


# -- 
# Run

if args.most_recent:
    ids = df_k.map(get_id).collect()
    min_dates = {}
    for i in ids: 
        try:
            mtc          = client.get(index='sub_aggregation', doc_type='fye', id=i)
            min_dates[i] = mtc['_source']['min_date']
        except:
            print 'missing \t %s' % i
    
    df_out = df_k.map(lambda x: merge_dates(x, min_dates))

elif args.from_scratch: 
    df_out = df_k


# -- 
# write to es

df_out.map(coerce_out).saveAsNewAPIHadoopFile(
    path='-',
    outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
    keyClass='org.apache.hadoop.io.NullWritable', 
    valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable', 
    conf={
        'es.input.json'      : 'false',
        'es.nodes'           : config['es']['host'],
        'es.port'            : str(config['es']['port']),
        'es.resource'        : '%s/%s' % ('sub_aggregation', 'fye'),
        'es.mapping.id'      : 'id',
        'es.write.operation' : 'upsert'
    }
)