#!/bin/bash
LOGFILE=/home/ubuntu/ernest/cronjobs/logs/omx_log

/home/ubuntu/ernest/scrape/scrape-omx-html.py \
        --config-path=/home/ubuntu/ernest/config.json \
        --start-page=20 \
        >> $LOGFILE

/home/ubuntu/ernest/enrich/enrich-ticker2cik.py \
        --index omx \
        --field-name tickers.symbol.cat
