from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan
from elasticsearch.helpers import reindex

import json
import argparse

# --
# CLI

parser = argparse.ArgumentParser(description='enrich-xbrl-rss-docs')
parser.add_argument("--year",  type=str, action='store')
parser.add_argument("--month",  type=str, action='store')
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

# -- 
# config 

config = json.load(open(args.config_path))

# -- 
# es connection

client = Elasticsearch([{"host" : config['es']['host'], "port" : config['es']['port']}])


# -- 
# global vars 

from_date = str(args.year + '-' + args.month + '-01')
to_date   = str(args.year + '-' + args.month + '-31')

query = { 
  "query" : { 
    "range" : { 
      "date" : { 
        "gte" : from_date,
        "lte" : to_date
      }
    }
  }
}

INDEX     = config['aq_forms_enrich']['index']
REF_INDEX = config['xbrl_rss']['index']
TYPE      = config['aq_forms_enrich']['_type']


def run(query): 
    for a in scan(client, index = INDEX, query = query): 
        res = client.search(index = REF_INDEX, body = {
            "query" : {
                "bool"  : { 
                    "must" : [
                    {
                        "match" : { 
                            "entity_info.dei_EntityCentralIndexKey.fact" : a["_source"]["cik"].zfill(10)
                        }
                    },
                    {
                        "match" : { 
                            "entity_info.dei_DocumentType.to_date" : a["_source"]["_enrich"]["period"]
                                }
                            }
                        ] 
                    }
                }
            })
        if res['hits']['total'] > 0:
            body = res['hits']['hits'][0]['_source']['facts']
            doc  = {
                    "__meta__" : { 
                        "financials" : get_financials( body )
                }
            }
        else: 
            doc = {
                "__meta__" : { 
                    "financials" : None
                }
            }
        yield {
            "_id"      : a['_id'],
            "_type"    : TYPE,
            "_index"   : INDEX,
            "_op_type" : "update",
            "doc"      : doc
        }



def get_financials( body ):
    out = { 
        'assets'                           : body.get("us-gaap_Assets", None),
        'liabilities'                      : body.get("us-gaap_Liabilities", None),
        'stockholdersEquity'               : body.get("us-gaap_StockholdersEquity", None),
        'netIncome'                        : body.get("us-gaap_NetIncomeLoss", None),
        'liabilitiesAndStockholdersEquity' : body.get("us-gaap_LiabilitiesAndStockholdersEquity", None),
        'liabilitiesCurrent'               : body.get("us-gaap_LiabilitiesCurrent", None),
        'assetsCurrent'                    : body.get("us-gaap_AssetsCurrent", None),
        'revenues'                         : body.get("us-gaap_Revenues", None)
    }
    return out


# --
# run

for a,b in streaming_bulk(client, run(query), chunk_size = 1000, raise_on_error = False):
    print a, b