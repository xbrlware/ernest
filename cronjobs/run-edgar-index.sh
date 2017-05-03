#!/bin/bash

# Ingest new filings from the edgar filings directory
# 
# Takes argument --most-recent to only grab new filings
# 
# Run each day to ensure index is current

now=$(date)
d=$(date +'%Y%m%d_%H%M%S')
python2.7 ../scrape/scrape-edgar.py \
        --most-recent \
        --back-fill \
        --date="$now" \
        --start-date="2010-01-01" \
        --section=both \
        --form-types=3,4 \
        --expected="$EXP" \
        --last-week\
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"
