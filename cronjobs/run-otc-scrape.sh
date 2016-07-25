#!/bin/bash

# Scrapes otc issuance and transactions from the FINRA otc directory and writes to 
# ernest_otc_raw
# 
# Takes arguments: 
#  --most-recent : limits the scope of the scrape to new documents
# 
# Run daily to ensure otc data is current


echo "run-otc-scrape.sh"
python ../scrape/scrape-otc.py --most-recent