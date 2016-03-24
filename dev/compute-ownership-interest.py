

#####################################Argument functionality


from pyspark import SparkContext
import argparse
import time
import json
from pprint import pprint
import re
from re import sub
import datetime
from datetime import date, timedelta

##define datetime variable

day = date.today() - timedelta(days = 9)
day2 = date.today() - timedelta(days = 365)
## define sc variable

sc = SparkContext()

###defining the interface

parser = argparse.ArgumentParser(description = 'grab_new_filings')

parser.add_argument('--from-scratch', dest = 'from_scratch', action = "store_true")

parser.add_argument('--last-week', dest = 'last_week', action = "store_true")

parser.add_argument('--last-year', dest = 'last_year', action = "store_true")

###getting arguments when the script is run

args = parser.parse_args()

if args.from_scratch: 
        print 'building ownership network from scatch'
        query = {
            "_source" : ["ownershipDocument.periodOfReport", "ownershipDocument.reportingOwner", "ownershipDocument.nonDerivativeTable", "ownershipDocument.nonDerivativeSecurity", "ownershipDocument.derivativeTable", "ownershipDocument.derivativeSecurity", "ownershipDocument.issuer"],
            "query" : {
                "match_all" :{}
            }
        }
elif args.last_week: 
    print 'updating ownership network'
    query = {
        "_source" : ["ownershipDocument.periodOfReport", "ownershipDocument.reportingOwner", "ownershipDocument.nonDerivativeTable", "ownershipDocument.nonDerivativeSecurity", "ownershipDocument.derivativeTable", "ownershipDocument.derivativeSecurity", "ownershipDocument.issuer"],
        "query": {
            "range" : {
              "periodOfReport" : {
                "gte" : str(day)
              }
            }
          }
    }
elif args.last_year: 
    print 'updating ownership network'
    query = {
        "_source" : ["ownershipDocument.periodOfReport", "ownershipDocument.reportingOwner", "ownershipDocument.nonDerivativeTable", "ownershipDocument.nonDerivativeSecurity", "ownershipDocument.derivativeTable", "ownershipDocument.derivativeSecurity", "ownershipDocument.issuer"],
        "query": {
            "range" : {
              "periodOfReport" : {
                "gte" : str(day2)
              }
            }
          }
    }
else: 
    print 'must chose one option [--from-scratch; --last-week]'



# ####### Development query last week:

# query = {
#     "_source" : ["ownershipDocument.periodOfReport", "ownershipDocument.reportingOwner", "ownershipDocument.nonDerivativeTable", "ownershipDocument.nonDerivativeSecurity", "ownershipDocument.derivativeTable", "ownershipDocument.derivativeSecurity", "ownershipDocument.issuer"],
#     "query": {
#         "range" : {
#           "periodOfReport" : {
#             "gte" : str(day)
#           }
#         }
#       }
# }




#####################################Build Rdd


config = {
    'hostname' : 'localhost',
    'hostport' : 9200,
    'resource_string' : 'forms/4,3'
 }




rdd_dup = sc.newAPIHadoopRDD(
    inputFormatClass = "org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass         = "org.apache.hadoop.io.NullWritable",
    valueClass       = "org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf             = {
        "es.nodes"    : config['hostname'],
        "es.port"     : str(config['hostport']),
        "es.resource" : config['resource_string'],
        "es.query"    : json.dumps(query)
   }
)



def get_cik(x):
    issuerCik = str(int((x[1]['ownershipDocument']['issuer']['issuerCik'])))
    key_string = x[0]
    re_cik = re.compile('edgar/data/\d{1,}/')
    sub_string = str(re.findall(re_cik, key_string))
    key_cik1 = str(re.sub('[a-z]', '', sub_string))
    key_cik2 = str(re.sub('\D', '', key_cik1))
    if key_cik2 == issuerCik: 
        return True
    else: 
        return False 


rdd = rdd_dup.filter(get_cik)

##rdd.cache() 

#####################################Define Functions Ownership Information
# Helpers

def clean_logical(x):
    if str(x).lower() == 'true':
        return 1
    if str(x).lower() == 'false':
        return 0
    else: 
        return x

# --
# Owner functions

def get_owner_sub(r):
    return {
        "isOfficer"         : clean_logical(r.get('reportingOwnerRelationship', {}).get('isOfficer', 0)),
        "isTenPercentOwner" : clean_logical(r.get('reportingOwnerRelationship', {}).get('isTenPercentOwner', 0)),
        "isDirector"        : clean_logical(r.get('reportingOwnerRelationship', {}).get('isDirector', 0)),
        "isOther"           : clean_logical(r.get('reportingOwnerRelationship', {}).get('isOther', 0)),
        "officerTitle"      : clean_logical(r.get('reportingOwnerRelationship', {}).get('officerTitle', 0)),
        "ownerName"         : clean_logical(r.get('reportingOwnerId', {}).get('rptOwnerName', 0)), 
        "ownerCik"          : clean_logical(r.get('reportingOwnerId',{}).get('rptOwnerCik', 0))
    }


def get_owner_fields(x):
    ro = x[1]['ownershipDocument']['reportingOwner'] # ro is either a list or an dictionary
    
    if isinstance(ro, dict): 
        single_multi_tag = 'single_owner'
    else: 
        single_multi_tag = 'multi_owner'
        
    top_level_fields = {
        "key"              :  x[0],
        "issuerCik"        :  x[1]['ownershipDocument']['issuer']['issuerCik'],
        "periodOfFiling"   :  x[1]['ownershipDocument']['periodOfReport'], 
        "single_multi_tag" :  single_multi_tag 
    }
    
    # Coerce ro to list
    if isinstance(ro, dict):
        ro = [ro]  
    
    ros = map(get_owner_sub, ro)
    
    for r in ros:
        r.update(top_level_fields)
    
    return ros


def structure_owners(x):
    return ( (x['key']), {
        'issuerCik'             : x['issuerCik'],
        'ownerCik'              : x['ownerCik'],
        'ownerName'             : x['ownerName'],
        'periodOfFiling'        : x['periodOfFiling'],
        'isDirector'            : x['isDirector'],
        'isOfficer'             : x['isOfficer'], 
        'officerTitle'          : x['officerTitle'],
        'isOther'               : x['isOther'], 
        'isTenPercentOwner'     : x['isTenPercentOwner'],
        'single_multi_tag'      : x['single_multi_tag'] 
    } )


# --
# Transaction functions
def detect_set(x, flag): 
    try:
        if flag == 'deriv2':
            val = x[1]['ownershipDocument']['derivativeTable']['derivativeTransaction']['transactionCoding']['transactionCode']  ##method 1     
        elif flag == 'deriv':
            val = x[1]['ownershipDocument']['derivativeSecurity']['transactionCoding']['transactionCode']
        elif flag == 'nderiv':
            val = x[1]['ownershipDocument']['nonDerivativeSecurity']['transactionCoding']['transactionCode']
        elif flag == 'nderiv2':
            val = x[1]['ownershipDocument']['nonDerivativeTable']['nonDerivativeTransaction']['transactionCoding']['transactionCode']
        
        return True
    
    except TypeError, e: 
       if str(e) == 'tuple indices must be integers, not str':
         return True
       if str(e) == "'NoneType' object has no attribute '__getitem__'":
         return False
       else:
         return 'unknown err'
    
    except KeyError, e: 
       return False
    
    except: 
       return False


def get_transactions_sub(r):
    trans_num_shares = r.get('transactionAmounts', {}).get('transactionShares', 0)
    trans_pps        = r.get('transactionAmounts', {}).get('transactionPricePerShare', 0)
    trans_a_d_code   = r.get('transactionAmounts', {}).get('transactionAcquiredDisposedCode', 0)
    post_trans_amt   = r.get('postTransactionAmounts', {}).get('sharesOwnedFollowingTransaction', 0)
    return {
        "transactionCode"                 : r.get('transactionCoding', {}).get('transactionCode', 0),
        "transactionAcquiredDisposedCode" : trans_a_d_code.get('value', 0)   if isinstance(trans_a_d_code, dict)   else trans_a_d_code,
        "transactionPricePerShare"        : trans_pps.get('value', 0)        if isinstance(trans_pps, dict)        else trans_pps, 
        "transactionShares"               : trans_num_shares.get('value', 0) if isinstance(trans_num_shares, dict) else trans_num_shares, 
        "sharesOwnedFollowingTransaction" : post_trans_amt.get('value', 0)   if isinstance(post_trans_amt, dict)   else post_trans_amt 
    }


def get_transactions(x): 
    top_level_fields = {
       'subset_id' : x[1]
    }
    
    if top_level_fields['subset_id'] == 'deriv': 
        ro = x[0]['ownershipDocument']['derivativeSecurity'] # ro is either a list or an dictionary
    elif top_level_fields['subset_id'] == 'deriv2': 
        ro = x[0]['ownershipDocument']['derivativeTable']['derivativeTransaction'] # ro is either a list or an dictionary
    elif top_level_fields['subset_id'] == 'nderiv': 
        ro = x[0]['ownershipDocument']['nonDerivativeSecurity'] # ro is either a list or an dictionary
    elif top_level_fields['subset_id'] == 'nderiv2': 
        ro = x[0]['ownershipDocument']['nonDerivativeTable']['nonDerivativeTransaction'] # ro is either a list or an dictionary
    
    # Coerce row to list
    if isinstance(ro, dict):
        ro = [ro]  
    
    ros = map(get_transactions_sub, ro)
    
    count = 0
    for r in ros:
        count = count + 1
        r['z_iter'] = str(count)
        r.update(top_level_fields)
    
    return ros


def coerce_(x): 
    tmp = { 
        'key' : x[0],
        # coerce data types for ownership information
        'isDirector'        : int(x[1][0]['isDirector']),
        'isOfficer'         : int(x[1][0]['isOfficer']),
        'isOther'           : int(x[1][0]['isOther']),
        'isTenPercentOwner' : int(x[1][0]['isTenPercentOwner']),
        'issuerCik'         : str(x[1][0]['issuerCik']),
    
        ##  'officerTitle' : x[1][0]['officerTitle'], ## may need to add some error handling stuff here
        'ownerCik'          : str(x[1][0]['ownerCik']),
        'ownerName'         : str(x[1][0]['ownerName']),
        'periodOfFiling'    : x[1][0]['periodOfFiling'],
    
        'single_multi_tag'  : str(x[1][0]['single_multi_tag']),
        'subset_id'         : str(x[1][1]['subset_id']),
        'z_iter'                          : int(x[1][1]['z_iter']),
        # coerce data types for transactional information
        'transactional' : {
            # 'z_iter'                          : int(x[1][1]['z_iter']),        
            # 'subset_id'                       : str(x[1][1]['subset_id']),
            'transactionCode'                 : str(x[1][1]['transactionCode']),
            'transactionShares'               : float(x[1][1]['transactionShares']),
            'transactionPricePerShare'        : float(x[1][1]['transactionPricePerShare']),
            'transactionAcquiredDisposedCode' : str(x[1][1]['transactionAcquiredDisposedCode']),
            'sharesOwnedFollowingTransaction' : float(x[1][1]['sharesOwnedFollowingTransaction']),
        }
    }
    # BKJ : !!! May need to coerce some other datatypes here -- doing it this way drops all of the "str" stuff !!! 
    tmp['id'] = '__'.join(str(tmp[k]) for k in ['key', 'issuerCik', 'ownerCik', 'single_multi_tag', 'subset_id','z_iter'])
    return ('-', tmp)


# --
# Run

owner_frame = rdd.flatMap(get_owner_fields).map(structure_owners)

union2 = sc.union([
    rdd.filter(lambda x: detect_set(x, flag = 'deriv')).map(lambda x:   ( (x[0]), (x[1], 'deriv') ) ),
    rdd.filter(lambda x: detect_set(x, flag = 'deriv2')).map(lambda x:  ( (x[0]), (x[1], 'deriv2') ) ),
    rdd.filter(lambda x: detect_set(x, flag = 'nderiv')).map(lambda x:  ( (x[0]), (x[1], 'nderiv') ) ),
    rdd.filter(lambda x: detect_set(x, flag = 'nderiv2')).map(lambda x: ( (x[0]), (x[1], 'nderiv2') ) ),
])

transaction_frame = union2.flatMapValues(get_transactions)
out = owner_frame.join(transaction_frame).map(coerce_)





write_index_name = 'ownership_network_map_revision'
write_index_type = 'draft'
path_to_id       = 'id'


out.saveAsNewAPIHadoopFile(
    path              = '-',
    outputFormatClass = "org.elasticsearch.hadoop.mr.EsOutputFormat",
    keyClass          = "org.apache.hadoop.io.NullWritable", 
    valueClass        = "org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf              = {
        "es.nodes"           : config['hostname'], # eg localhost
        "es.port"            : str(config['hostport']), # eg 9200
        "es.resource"        : write_index_name + "/" + write_index_type,
        "es.mapping.id"      : path_to_id, # optional
        "es.write.operation" : "upsert" # create if id doesn't exist, replace if it does
    }
)
