#!/bin/bash

# Uses edgar FTP to download ownership documents to forms index
# 
# Takes arguments: 
#  --back-fill : try and download all documents that havent been tried or have failed
#  --start-date: start date for back fill ingest
#  --section   : header, body or both
#  --form-types: type of filing to download 
# 
# Run each day after the edgar index script has been populated with new filings


IN=$(curl -XGET 'localhost:9205/ernest_forms_cat/_count?pretty' | jq '.count') 

EXP=$(curl -XGET localhost:9205/edgar_index_cat/_count -d '{"query": {"bool": {"must": [{"terms": {"form.cat": [3, 4]}},
                             {"range": {"date": {"gte": "2010-01-01",
                                                 "lte": "2016-08-23"}}},
                             {"bool": {"minimum_should_match": 1,
                                       "should": [{"filtered": {"filter": {"or": [{"missing": {"field": "download_try2"}},
                                                                                  {"missing": {"field": "download_try_hdr"}}]}}},
                                                  {"bool": {"must": [{"match": {"download_success2": false}},
                                                                     {"range": {"try_count_body": {"lte": 6}}}]}},
                                                  {"bool": {"must": [{"match": {"download_success_hdr": false}},
                                                                     {"range": {"try_count_hdr": {"lte": 6}}}]}}]}}]}}}' | jq '.count')

echo "run-edgar-forms"
python ../scrape/scrape-edgar-forms.py --back-fill \
    --start-date="2010-01-01" \
    --section=both \
    --form-types=3,4 

OUT=$(curl -XGET 'localhost:9205/ernest_forms_cat/_count?pretty' | jq '.count') 

now=$(date)

index="ernest-forms-cat"

python ../enrich/generic-meta-enrich.py --index="$index" --date=now --count-in="$IN" --count-out="$OUT" --expected="$EXP"