#!/bin/bash

# Normalize time scale for financials values 
# 
# Takes argument --most-recent to only run for documents that havent been scaled
# 
# Run each day to ensure index is current

echo "run-enrich-normalize-financials"
python ../enrich/enrich-normalize-financials.py --most-recent