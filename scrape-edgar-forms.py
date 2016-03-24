import re, urllib2, xmltodict, time, json, argparse
from datetime import datetime

from ftplib import FTP
from datetime import date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, streaming_bulk

# *** This is going to fail -- where is this file? ***
#from sec_ftp import *
# from sec_header_ftp_download import *

day = date.today() - timedelta(days = 9)

# --
# Helpers
def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")

# --
# Defining command line interface

parser.add_argument("--back-fill", action = 'store_true') 
parser.add_argument("--start-date", type = str, action = 'store')
parser.add_argument("--end-date", type = str, action = 'store')  
parser.add_argument("--form-types", type = str, action = 'store')
parser.add_argument("--section", type = str, action = 'store')

args = parser.parse_args()

print args

# # -- 
# # Config
# config  = json.load(open(config_path))


# HOSTNAME = config['elasticsearch']['hostname']
# HOSTPORT = config['elasticsearch']['hostport']

# FORMS_INDEX = config['elasticsearch']['forms_index']
# INDEX_INDEX = config['elasticsearch']['index_index']


# # -- 
# # IO

# s        = FTP('ftp.sec.gov', 'anonymous')
# sec      = SECFTP(s)
# client   = Elasticsearch([{'host' : HOSTNAME, 'port' : HOSTPORT}])



# # --
# # _ default end date behavior

# if not args.end_date: 
#     args.end_date = day = date.today()


# params = {
#     'back_fill'    : args.back_fill,
#     'start_date'   : datetime.strptime(args.start_date, '%Y-%m-%d'),
#     'end_date'     : args.end_date,
#     'form_types'   : map(int, args.form_types.split(',')),
#     'section'      : args.section
# }




# docs   = params['section'] in ['body', 'both']
# header = params['section'] in ['header', 'both']
# if (not docs) and (not header):
#     raise Exception('section must be in [body, header, both]')

# must = []
# must.append({ "terms" : {"form" : params['form_types']} })

# must.append({ "range" : {"date" : {"gte" : params['start_date'], "lte" : params['end_date']}}})


# if params['back_fill']:
#     must.append({
#         "bool" : {
#             "should" : [
#                 {
#                     "match" : {"download_success2" : False }
#                 },
#                 {
#                     "match" : {"download_success_hdr" : False}
#                 }, 
#                 {
#                     "filtered" : {
#                         "filter" : { 
#                             "missing" : { 
#                                 "field" : "download_try2"
#                             }
#                         }
#                     }
#                 }, 
#                 {
#                     "filtered" : {
#                         "filter" : { 
#                             "missing" : { 
#                                 "field" : "download_try_hdr"
#                             }
#                         }
#                     }
#                 }
#             ],
#             "minimum_should_match" : 1
#         }    
#     })



# query = {"query" : {"bool" : {"must" : must}}}


# print(query)

# # --
# # Function definitions

# # Header scraper
# def get_headers(a, forms_index = FORMS_INDEX, index_index = INDEX_INDEX):
#     path = sec.url_to_path(a['_id'])
#     out  = {
#         "_id"      : a['_id'],
#         "_type"    : a['_type'],
#         "_index"   : forms_index,
#         "_op_type" : 'update'
#     }
#     out_log = {
#         "_id"       : a['_id'],
#         "_type"     : "entry",
#         "_index"    : index_index, 
#         "_op_type"  : "update"
#     }
#     try:
#         payload = { "header" : sec.download_parsed(path)} 
#         out['doc']    = payload
#         out['upsert'] = payload 
#         out_log['doc'] = {"download_try_hdr" : True, "download_success_hdr" : True}
#         return out, out_log  
#     except (KeyboardInterrupt, SystemExit):
#         raise      
#     except:
#         out_log['doc'] = {"download_try_hdr" : True, "download_success_hdr" : False}
#         print 'failed @ ' + path
#         return None, out_log


# # Doc scraper
# def get_docs(a, forms_index = FORMS_INDEX, index_index = INDEX_INDEX):
#     out = {
#         "_id"      : a['_id'],
#         "_type"    : a['_type'],
#         "_index"   : forms_index,
#         "_op_type" : "update"
#     }
    
#     out_log = {
#         "_id"       : a['_id'],
#         "_type"     : "entry",
#         "_index"    : index_index, 
#         "_op_type"  : "update"
#     }
    
#     try:
#         page           = sec.download(a['_id'])
#         split_string   = 'ownershipDocument>'
#         page           = '<' + split_string + page.split(split_string)[1] + split_string
#         page           = re.sub('\n', '', page)
#         page           = re.sub('>([0-9]{4}-[0-9]{2}-[0-9]{2})-[0-9]{2}:[0-9]{2}<', '>\\1<', page)
#         page           = re.sub('([0-9]{2})(- -)([0-9]{2})', '\\1-\\3', page) 
#         parsed_page    = xmltodict.parse(page)
#         out['doc']     = parsed_page
#         out['upsert']  = parsed_page
        
#         out_log['doc'] = {"download_try2" : True, "download_success2" : True}
#         return out, out_log
#     except (KeyboardInterrupt, SystemExit):
#         raise
#     except:
#         out_log['doc'] = {"download_try2" : True, "download_success2" : False}
#         print 'failed @ ' + a['_id']
#         return None, out_log



# def get_data(query, docs, header, index_index = INDEX_INDEX):
#     for a in scan(client, index = index_index, query = query):
#         if docs:
#             out, out_log = get_docs(a)    
#             if out:
#                 yield out
            
#             yield out_log
        
#         if header:
#             out, out_log = get_headers(a)
#             if out: 
#                 yield out

#             yield out_log


# # -- 
# # Run scraper
# for a,b in streaming_bulk(client, get_data(query, docs, header), chunk_size = 100):
#     print a, b
