#!/bin/bash

# Takes new documents from the forms index and incorporates them into the ownership 
# aggregation index
# 
# Takes arguments: 
#  --last-week : limits the scope of the enrichment to documents from the week before
# 
# Run daily to ensure the index is current

now=$(date)
d=$(date +'%Y%m%d_%H%M%S')
        
python2.7 ../enrich/compute-ownership.py \
        --last-week \
        --date="$now" \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"
