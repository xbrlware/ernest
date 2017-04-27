#!/bin/bash

# Normalize time scale for financials values 
# 
# Takes argument --most-recent to only run for documents that havent been scaled
# 
# Run each day to ensure index is current
d=$(date +'%Y%m%d_%H%M%S')
python ../enrich/enrich-normalize-financials.py \
        --most-recent \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_run_enrich_normaliz_financials_$d"
