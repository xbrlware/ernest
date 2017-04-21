#!/bin/bash

# Add entry to `crontab` like
# 0 1 * * * /home/ubuntu/ernest/crontabs/_run-all.sh /home/ubuntu/ernest

ERNEST_PATH=$1

d=$(date +'%Y%m%d_%H%M%S')
LOGFILE=$ERNEST_PATH/cronjobs/logs/log_$d

cd $ERNEST_PATH/cronjobs/ && bash run-edgar-index.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-ownership.sh

cd $ERNEST_PATH/cronjobs/ && bash run-otc-scrape.sh
cd $ERNEST_PATH/cronjobs/ && bash run-finra-scrapes.sh

cd $ERNEST_PATH/cronjobs/ && bash run-halts-process.sh

cd $ERNEST_PATH/cronjobs/ && bash run-add-otc-flag.sh

cd $ERNEST_PATH/cronjobs/ && bash run-xbrl-submission-scrape.sh 

cd $ERNEST_PATH/cronjobs/ && bash run-build-delinquency.sh

cd $ERNEST_PATH/cronjobs/ && bash run-xbrl.sh

cd $ERNEST_PATH/cronjobs/ && bash run-compute-delinquency.sh

cd $ERNEST_PATH/cronjobs/ && bash run-compute-fye-graph.sh
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-aqfs-fye.sh >> $LOGFILE  # error
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-normalize-financials.sh >> $LOGFILE # error

# echo "-- update nt filings index and enrich financials documents --"
# cd $ERNEST_PATH/cronjobs/ && bash run-build-nt-filings.sh >> $LOGFILE # error
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-ntfilings-period.sh >> $LOGFILE # error
# cd $ERNEST_PATH/cronjobs/ && bash run-add-nt-filings-tag.sh >> $LOGFILE # error

# echo "-- scrape and enrich touts --"
# cd $ERNEST_PATH/cronjobs/ && bash run-scrape-touts.sh >> $LOGFILE 
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-touts.sh >> $LOGFILE 

# echo "-- enrich crowdsar data & update pv index --"
cd $ERNEST_PATH/investor-forums/cronjobs/ && bash run-daily.sh 

# echo "-- aggregations --"
# cd $ERNEST_PATH/aggregations && bash run-all.sh
