#!/bin/bash
LOGFILE=/home/ubuntu/ernest/cronjobs/logs/omx_log

FIRST_PHANTOMS=$(ps -aux | grep phantomjs | awk '{print $2}')

/home/ubuntu/ernest/scrape/scrape-omx-html.py \
        --config-path=/home/ubuntu/ernest/config.json \
        --start-page=20 \
        >> $LOGFILE

SECOND_PHANTOMS=$(ps -aux | grep phantomjs | awk '{print $2}')

kill $(echo `echo ${SECOND_PHANTOMS[@]} ${FIRST_PHANTOMS[@]} | tr ' ' '\n' | sort | uniq -u`)

/home/ubuntu/ernest/enrich/enrich-ticker2cik.py \
        --index omx \
        --field-name tickers.symbol.cat
