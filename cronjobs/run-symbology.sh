#!/bin/bash

# Takes documents from indices edgar_index_cat and ernest_forms_cat and incorporates 
# them into the symbology aggregation index. 
# 
# The first script takes all non ownership filings from edgar_index_cat and incorporates 
# them into the symbology aggregation. 
# 
# The second takes ownership documents from ernest_forms_cat and incorprates them into the 
# symbology aggregation index 
# 
# Takes arguments: 
#  --last-week : limits the scope of the enrichment to filings from the previous week
# 
# Run daily to ensure data in the symbology index is current

# IN=$(curl -XGET 'localhost:9205/ernest_symbology_v2/_count?pretty' | jq '.count') 
# python2.7 ../enrich/compute-index2symbology.py --last-week

python2.7 ../enrich/compute-ownership2symbology.py --last-week

# OUT=$(curl -XGET 'localhost:9205/ernest_symbology_v2/_count?pretty' | jq '.count') 

# now=$(date)

# index="ernest-symbology-v2"

# python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 
