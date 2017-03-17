#!/bin/bash

# Ingest new filings from the edgar filings directory
# 
# Takes argument --most-recent to only grab new filings
# 
# Run each day to ensure index is current

echo "run-edgar-index"

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

now=$(date)
python ../scrape/scrape-edgar.py \
        --most-recent \
        --back-fill \
        --date="$now" \
        --start-date="2010-01-01" \
        --section=both \
        --form-types=3,4 \
        --expected="$EXP"
