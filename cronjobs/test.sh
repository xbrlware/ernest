#!/bin/bash

# Ingest new filings from the edgar filings directory
# 
# Takes argument --most-recent to only grab new filings
# 
# Run each day to ensure index is current

IN=$(curl -XGET 'localhost:9205/edgar_index_cat/_count?pretty' | jq '.count') 


echo "run-edgar-index"
python ../scrape/scrape-edgar-index.py --most-recent

OUT=$(curl -XGET 'localhost:9205/edgar_index_cat/_count?pretty' | jq '.count') 

now=$(date)

index="edgar-index-cat"

echo $index
echo $now
echo $IN
echo $OUT

# python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 