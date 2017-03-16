#!/bin/bash

# Ingest new filings from the edgar filings directory
# 
# Takes argument --most-recent to only grab new filings
# 
# Run each day to ensure index is current

echo "run-edgar-index"

now=$(date)
index="edgar-index-cat"

python ../scrape/scrape-edgar.py --most-recent --index="$index" --date="$now" 
