#!/bin/bash

# This script adds and 'is_otc' flag to the symbology and ownership aggregation indices
# 
# Takes arguments: 
#  --field-name : the name of the field with the ticker in it for the given index (sym or own)
#  --index      : the name of the index to be enriched with flag (symbology or ownership)
# 
# Run daily to ensure new otc data is included in aggregation indices

now=$(date)
d=$(date +'%Y%m%d_%H%M%S')

python ../enrich/enrich-add-otc-flag.py \
        --index ownership \
        --field-name issuerTradingSymbol \
        --date="$now" \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"

python ../enrich/enrich-add-otc-flag.py \
        --index symbology \
        --field-name ticker \
        --date="$now" \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"
