#!/bin/bash

ERNEST_PATH=/home/ubuntu/ernest


echo 'download xbrl documents'

python ../scrape/xbrl-download.py \
    --year=$1 \
    --month=$2


echo 'parse & ingest xbrl documents'

cd $ERNEST_PATH/cronjobs/ && bash run-parse-draft.sh $1 $2


echo 'enrich xbrl documents'

python ../enrich/xbrl-rss-enrich.py \
    --year=$1 \
    --month=$2


echo 'interpolating xbrl documents' \

python ../enrich/xbrl-rss-interpolation.py

