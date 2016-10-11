#!/bin/bash

# Ingest new touts from stockreads
# 
# Takes argument --most-recent to only grab new touts
# 
# Run each day to ensure index is current

IN=$(curl -XGET 'localhost:9205/ernest_touts/_count?pretty' | jq '.count') 

echo "run-scrape-touts"
python ../scrape/1_stockreads_scrape.py --most-recent

OUT=$(curl -XGET 'localhost:9205/ernest_touts/_count?pretty' | jq '.count') 
now=$(date)
index="ernest-touts"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 