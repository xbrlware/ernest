import json
import urllib2
import argparse
import re
from datetime import datetime, date, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


# -- 
# CLI

parser = argparse.ArgumentParser(description='Generic Monitoring Index')
parser.add_argument('--index', type=str, dest='index', action="store")
parser.add_argument('--expected', type=str, dest='expected', action="store")
parser.add_argument('--count-in', type=str, dest='count_in', action="store")
parser.add_argument('--count-out', type=str, dest='count_out', action="store")
parser.add_argument('--date', type=str, dest='date', action="store")
parser.add_argument('--config-path', type=str, action='store', default='/home/ubuntu/ernest/config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))
client = Elasticsearch([
    {"host" : config['es']['host'], 
    "port" : config['es']['port']}
], timeout=6000)


if not args.expected: 
    expected = None
else: 
    expected = args.expected

month_lookup = {
  'jan' : 1, 
  'feb' : 2,
  'mar' : 3,
  'apr' : 4, 
  'may' : 5, 
  'jun' : 6,
  'jul' : 7, 
  'aug' : 8,
  'sep' : 9, 
  'oct' : 10, 
  'nov' : 11, 
  'dec' : 12
}

# --
# Functions

def to_date(date): 
    d      = re.compile('\s{1}\d{1,2}\s{1}')
    d1     = re.findall(d, date)[0]
    d2     = re.sub('\D', '', d1).zfill(2)
    month  = date[:7][4:].lower()
    month2 = str(month_lookup[month])
    month3 = month2.zfill(2)
    year   = date[-4:]
    date2  = year + '-' + month3 + '-' + d2
    return date2


def buildOut(): 
    body = {
        "index" : args.index, 
        "expected" : expected,
        "count_in" : args.count_in, 
        "count_out" : args.count_out, 
        "date"      : to_date(args.date)
    }
    return body



client.index(
    index    = 'ernest_performance_graph2', 
    doc_type = 'execution', 
    id       = args.index + "__" + re.sub('-', '', to_date(args.date)),
    body     = buildOut()
)
