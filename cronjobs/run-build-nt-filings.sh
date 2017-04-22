#!/bin/bash

# Ingest new nt filings documents into ernest_nt_filings index
# 
# Takes argument --most-recent to only grab new filings
# 
# Run each day to ensure index is current

d=$(date +'%Y%m%d_%H%M%S')
python ../enrich/nt_filings.py \
    --most-recent \
    --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"

