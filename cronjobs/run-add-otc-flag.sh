#!/bin/bash

# This script adds and 'is_otc' flag to the symbology and ownership aggregation indices
# 
# Takes arguments: 
#  --field-name : the name of the field with the ticker in it for the given index (sym or own)
#  --index      : the name of the index to be enriched with flag (symbology or ownership)
# 
# Run daily to ensure new otc data is included in aggregation indices

echo "running enrich-add-otc-flag"

python ../enrich/enrich-add-otc-flag.py --index ownership --field-name issuerTradingSymbol
python ../enrich/enrich-add-otc-flag.py --index symbology --field-name ticker
