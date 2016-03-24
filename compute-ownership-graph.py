from pyspark import SparkContext
import argparse, time, json, re, datetime
from pprint import pprint
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk, scan

sc = SparkContext()

# --
# define CLI
parser = argparse.ArgumentParser(description = 'grab_new_filings')
parser.add_argument('--from-scratch', dest = 'from_scratch', action = "store_true")
parser.add_argument('--last-week',    dest = 'last_week',    action = "store_true")
parser.add_argument("--config-path",   type = str, action = 'store')
args = parser.parse_args()

config = json.load(open(args.config_path))
day    = date.today() - timedelta(days = 9)

if args.from_scratch: 
        print 'building ownership network from scatch'
        query = {
            "_source" : ["ownershipDocument.periodOfReport", "ownershipDocument.reportingOwner", "ownershipDocument.issuer"],
            "query": {
                "bool" : { 
                    "must" : [
                    {
                        "match_all" : {} 
                    },
                    {
                        "filtered" : {
                            "filter" : {
                                "exists" : {
                                    "field" : "ownershipDocument"
                                }
                            }
                        }
                    }             
                    ]
                } 
            }
        }

elif args.last_week: 
    print 'updating ownership network'
    query = {
        "_source" : ["ownershipDocument.periodOfReport", "ownershipDocument.reportingOwner", "ownershipDocument.issuer"],
            "query": {
                "bool" : { 
                    "must" : [
                    {
                        "range" : {
                            "periodOfReport" : {
                                "gte" : str(day)
                            }
                        }
                    },
                    {
                        "filtered" : {
                            "filter" : {
                                "exists" : {
                                    "field" : "ownershipDocument"
                                }
                            }
                        }
                    }             
                    ]
                } 
            }
        }
else: 
    print 'must chose one option [--from-scratch; --last-week]'


# --
# Connections

client = Elasticsearch([{
    'host' : config["elasticsearch"]["hostname"], 
    'port' : config["elasticsearch"]["hostport"]
}], timeout = 60000)

rdd = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass         = "org.apache.hadoop.io.NullWritable",
    valueClass       = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf             = {
        "es.nodes"    : config['elasticsearch']['hostname'],
        "es.port"     : str(config['elasticsearch']['hostport']),
        "es.resource" : "%s/%s" % (config['elasticsearch']['forms']['index'], config['elasticsearch']['forms']['_type']),
        "es.query"    : json.dumps(query)
   }
)

# --
# function definition

def clean_logical(x):
    if str(x).lower() == 'true':
        return 1
    if str(x).lower() == 'false':
        return 0
    else: 
        return x


def _get_owners(r):
    return {
        "isOfficer"         : clean_logical(r.get('reportingOwnerRelationship', {}).get('isOfficer', 0)),
        "isTenPercentOwner" : clean_logical(r.get('reportingOwnerRelationship', {}).get('isTenPercentOwner', 0)),
        "isDirector"        : clean_logical(r.get('reportingOwnerRelationship', {}).get('isDirector', 0)),
        "isOther"           : clean_logical(r.get('reportingOwnerRelationship', {}).get('isOther', 0)),
        "officerTitle"      : clean_logical(r.get('reportingOwnerRelationship', {}).get('officerTitle', 0)),
        "ownerName"         : clean_logical(r.get('reportingOwnerId', {}).get('rptOwnerName', 0)), 
        "ownerCik"          : clean_logical(r.get('reportingOwnerId',{}).get('rptOwnerCik', 0))
    }


def get_owners(val):
    top_level_fields = {
        "issuerCik"      :  val['ownershipDocument']['issuer']['issuerCik'],
        "periodOfFiling" :  val['ownershipDocument']['periodOfReport']
    }
    
    ro = val['ownershipDocument']['reportingOwner'] # ro is either a list or an dictionary
    ro = [ro] if type(ro) == type({}) else ro
    
    ros = map(_get_owners, ro)
    for r in ros:
        r.update(top_level_fields)
    
    return ros


def get_info(x): 
    tmp = {
        "issuerCik"         : str(x[1]['issuerCik']), 
        "periodOfFiling"    : x[1]['periodOfFiling'],
        "ownerName"         : str(x[1]['ownerName']),
        "ownerCik"          : str(x[1]['ownerCik']),
        "isDirector"        : int(x[1]['isDirector']),
        "isOfficer"         : int(x[1]['isOfficer']),
        "isOther"           : int(x[1]['isOther']),
        "isTenPercentOwner" : int(x[1]['isTenPercentOwner']),
        "officerTitle"      : str(x[1]['officerTitle']) ##have taken this out of the output variable becuase of consistency issues
    }
    return ( (tmp['issuerCik'], tmp['ownerName'], tmp['ownerCik'], tmp['isDirector'], tmp['isOfficer'], tmp['isOther'], tmp['isTenPercentOwner']), tmp['periodOfFiling'] )


def coerce_out(x): 
    tmp = {
        "issuerCik"         : str(x[0][0]), 
        "ownerName"         : str(x[0][1]),
        "ownerCik"          : str(x[0][2]),
        "isDirector"        : int(x[0][3]),
        "isOfficer"         : int(x[0][4]),
        "isOther"           : int(x[0][5]),
        "isTenPercentOwner" : int(x[0][6]),
        ##"officerTitle"        : str(x[0][7]),
        "min_date"          : str(x[1][0]),
        "max_date"          : str(x[1][1])
    }
    ##tmp['id'] = str(tmp['issuerCik']) + '__' + str(re.sub(' ', '_', tmp['ownerName'])) + '__' + str(tmp['ownerCik']) + '__' + str(tmp['isDirector']) + '__' + str(tmp['isOfficer']) + '__' + str(tmp['isOther']) + '__' + str(tmp['isTenPercentOwner']) + '__' + str(re.sub(' ', '_', tmp['officerTitle']))
    tmp['id'] = str(tmp['issuerCik']) + '__' + str(re.sub(' ', '_', tmp['ownerName'])) + '__' + str(tmp['ownerCik']) + '__' + str(tmp['isDirector']) + '__' + str(tmp['isOfficer']) + '__' + str(tmp['isOther']) + '__' + str(tmp['isTenPercentOwner'])  ##+ '__' + re.sub(' ', '_', str(tmp['officerTitle']))
    return ('-', tmp)


def get_ids(x): 
    idx = str(x[0][0]) + '__' + str(re.sub(' ', '_', str(x[0][1]))) + '__' + str(x[0][2]) + '__' + str(x[0][3]) + '__' + \
            str(x[0][4]) + '__' + str(x[0][5]) + '__' + str(x[0][6])
    return str(idx) 


def _merge(x): 
    idx = str(x[0][0]) + '__' + str(re.sub(' ', '_', str(x[0][1]))) + '__' + str(x[0][2]) + '__' + str(x[0][3]) + '__' + \
            str(x[0][4]) + '__' + str(x[0][5]) + '__' + str(x[0][6])
    doc = [i for i in match_ids if i["_id"] == str(idx)]
    if len(doc) == 0: 
        out = x
    elif len(doc) == 1: 
        x[1][0] = doc[0]['_source']['min_date']
        out = x
    else: 
        print('multiple docs same id')
    return out

# --
# Apply pipeline
owners = rdd.flatMapValues(get_owners)\
    .map(get_info)\
    .groupByKey()\
    .mapValues(lambda x: [min(x), max(x)])


if args.last_week: 
    id_set = owners_range.map(get_ids)
    idvec  = id_set.collect()  
    match_ids = []
    for i in idvec: 
        query = { "query" : { "match" : {  "_id" : str(i) } } }
        for a in scan(client, index = config['ownership']['index'], query = query): 
            match_ids.append(a)
    
    owners_out = owners_range.map(_merge)
elif args.from_scratch: 
    owners_out = owners_range


# --
# write to ES

path_to_id = 'id'
owners_out.map(coerce_out).saveAsNewAPIHadoopFile(
    path              = '-',
    outputFormatClass = "org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass          = "org.apache.hadoop.io.NullWritable", 
    valueClass        = "org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf              = {
        "es.nodes"           : config['elasticsearch']['hostname'],
        "es.port"            : str(config['elasticsearch']['hostport']),
        "es.resource"        : '%s/%s' % (config['ownership']['index'], config['ownership']['_type'])
        "es.mapping.id"      : path_to_id,
        "es.write.operation" : "upsert"
    }
)
