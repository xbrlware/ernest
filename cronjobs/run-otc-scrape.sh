#!/bin/bash

# Scrapes otc issuance and transactions from the FINRA otc directory and writes to 
# ernest_otc_raw
# 
# Takes arguments: 
#  --most-recent : limits the scope of the scrape to new documents
# 
# Run daily to ensure otc data is current

now=$(date)
d=$(date +'%Y%m%d_%H%M%S')
python2.7 ../scrape/scrape-otc.py \
        --most-recent \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"
