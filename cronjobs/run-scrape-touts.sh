#!/bin/bash

# Ingest new touts from stockreads
# 
# Takes argument --most-recent to only grab new touts
# 
# Run each day to ensure index is current

python ../scrape/1_stockreads_scrape.py --from-scratch
