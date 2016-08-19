#!/bin/bash

# Add period to NT filings index 
# 
# Run each day to ensure index is current

echo "run-enrich-ntfilings-period"
python ../enrich/enrich-ntfilings-period.py 