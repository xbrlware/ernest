#!/bin/bash
LOGFILE=/home/ubuntu/ernest/cronjobs/logs/omx_log

python2.7 /home/ubuntu/ernest/scrape/scrape-omx.py \
        --config-path=/home/ubuntu/ernest/config.json \
        --start-page=20 \
        --index omx \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d" \
        --ticker-to-cik-field-name tickers.symbol.cat
