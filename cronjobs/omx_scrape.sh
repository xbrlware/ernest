#!/bin/bash
LOGFILE=/home/ubuntu/ernest/cronjobs/logs/omx_log

/home/ubuntu/ernest/scrape/scrape-omx-html.py \
        --config-path=/home/ubuntu/ernest/config.json \
        --start-page=20 \
        --index omx \
        --ticker-to-cik-field-name tickers.symbol.cat >> $LOGFILE
