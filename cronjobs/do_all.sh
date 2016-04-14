#!/bin/bash

ERNEST_PATH=/home/ubuntu/ernest
# ^^ This should get moved to crontab and passed in as $1

cd $ERNEST_PATH/cronjobs/ && bash run-edgar-index.sh
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-forms.sh
cd $ERNEST_PATH/cronjobs/ && bash run-symbology.sh
cd $ERNEST_PATH/cronjobs/ && bash run-ownership.sh
cd $ERNEST_PATH/investor-forums/cronjobs/ && bash run-daily.sh 
