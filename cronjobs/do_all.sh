#!/bin/bash

ERNEST_PATH=/home/ubuntu/ernest
# ^^ This should get moved to crontab and passed in as $1

cd $ERNEST_PATH/cronjobs/ && bash run-edgar-index.sh $ERNEST_PATH/config.json
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-forms.sh $ERNEST_PATH/config.json
cd $ERNEST_PATH/cronjobs/ && bash run-symbology.sh $ERNEST_PATH/config.json
cd $ERNEST_PATH/cronjobs/ && bash run-ownership.sh $ERNEST_PATH/config.json
cd $ERNEST_PATH/cronjobs/ && bash run-otc-scrape.sh $ERNEST_PATH/config.json
cd $ERNEST_PATH/investor-forums/cronjobs/ && bash run-daily.sh $ERNEST_PATH/config.json
