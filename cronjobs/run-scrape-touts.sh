#!/bin/bash

# Ingest new touts from stockreads
# 
# Takes argument --most-recent to only grab new touts
# 
# Run each day to ensure index is current

echo "run-scrape-touts"
python ../scrape/1_stockreads_scrape.py --most-recent
