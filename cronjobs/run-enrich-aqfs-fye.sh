#!/bin/bash

# Enrich aqfs documents with FYE from xbrl submissions & aggregation index
# 
# Run each day to ensure index is current
d=$(date +'%Y%m%d_%H%M%S')

python ../enrich/compute-aqfs-fye.py \
        --sub \
        --function \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"
