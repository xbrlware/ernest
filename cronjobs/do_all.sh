#!/bin/bash

# Add entry to `crontab` like
# 0 1 * * * /home/ubuntu/ernest/crontabs/do_all.sh /home/ubuntu/ernest
# 

ERNEST_PATH=$1

echo "-- edgar data ingestion --"
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-index.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-forms.sh 

echo "-- enrich edgar aggregation indices --"
cd $ERNEST_PATH/cronjobs/ && bash run-symbology.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-ownership.sh 

echo "-- run otc scrapes --"
cd $ERNEST_PATH/cronjobs/ && bash run-otc-scrape.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-finra-scrapes.sh

echo "-- enrich otc, symbology & ownership --"
cd $ERNEST_PATH/cronjobs/ && bash run-otc-enrich.sh
cd $ERNEST_PATH/cronjobs/ && bash run-sic-enrich.sh

echo "-- get new xbrl sub docs if available --" 
cd $ERNEST_PATH/cronjobs/ && bash run-xbrl-submission-scrape.sh

echo "-- update & compute delinquency --"
cd $ERNEST_PATH/cronjobs/ && bash run-build-delinquency.sh
cd $ERNEST_PATH/cronjobs/ && bash run-compute-delinquency.sh

echo "-- enrich crowdsar data & update pv index --"
cd $ERNEST_PATH/investor-forums/cronjobs/ && bash run-daily.sh
