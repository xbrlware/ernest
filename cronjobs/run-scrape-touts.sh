#!/bin/bash

# Ingest new touts from stockreads
# 
# Takes argument --most-recent to only grab new touts
# 
# Run each day to ensure index is current

python2.7 ../scrape/scrape-stockreads.py --most-recent
