#!/bin/bash

# Aggregate FYE schedule for finacials documents
# 
# Takes argument --most-recent to only incorporate new filings into aggregation
# 
# Run each day to ensure index is current
d=$(date +'%Y%m%d_%H%M%S')
logfile="/home/ubuntu/ernest/cronjobs/logs/log_$d"

python2.7 ../enrich/compute-fye-graph.py \
        --most-recent \
        --log-file=$logfile
