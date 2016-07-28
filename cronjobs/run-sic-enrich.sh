#!/bin/bash

# Script adds text description of SIC code accompanying each entry in symbology and 
# ownerhsip indices
# 
# Takes argument: 
#  --index : name of index to enrich with SIC description (symbology or ownership)
# 
# Run daily to ensure all summary data for companies and owners is up to date where 
# that information is available

echo 'run-sic-enrich'

echo '\t adding sic info to symbology documents'
python ../enrich/enrich-add-sic-descs.py --index='symbology'

echo '\t adding sic info to ownership documents'
python ../enrich/enrich-add-sic-descs.py --index='ownership'
