#!/bin/bash

# Scrapes otc issuance and transactions from the FINRA otc directory and writes to 
# ernest_otc_raw
# 
# Takes arguments: 
#  --most-recent : limits the scope of the scrape to new documents
# 
# Run daily to ensure otc data is current

IN=$(curl -XGET 'localhost:9205/ernest_otc_raw_cat/_count?pretty' | jq '.count') 

echo "run-otc-scrape"
python ../scrape/scrape-otc.py --most-recent

OUT=$(curl -XGET 'localhost:9205/ernest_otc_raw_cat/_count?pretty' | jq '.count') 

now=$(date)

index="ernest-otc-raw-cat"

python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 