#!/bin/bash

# Scrape and ingest new trading suspensions from the SEC and FINRA otc json page
# 
# This shell process includes four scripts: 
# 
# scrape-sec-suspensions: 
#   --scrape and ingest new SEC suspensions
# scrape-finra-directories: 
#   --scrape and ingest new trading halts from the FINRA otc json page
# enrich-halt-date: 
#   --coerce halt date for otc halts to yyyy-mm-dd from unix timestamp
# merge-halts.py: 
#   --merge otc halts into sec halts index using high level entity resolution criteria
# 
# Run each day to ensure index is current
now=$(date)
d=$(date +'%Y%m%d_%H%M%S')

python2.7 ../scrape/scrape-halt.py \
        --most-recent \
        --halts \
        --index='suspension' \
        --name-to-cik-field-name='company' \
        --ticker-to-cik-field-name='__meta__.finra.ticker' \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d" \
        --date="$now"
