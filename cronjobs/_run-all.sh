#!/bin/bash

# Add entry to `crontab` like
# 0 1 * * * /home/ubuntu/ernest/crontabs/_run-all.sh /home/ubuntu/ernest
# 

# 1) have to fix the index convention and switch it to script, target index, args
# 2) have to add time start time stop
# 3) also have to go and write reasonable queries for some of the more involved stepwise process

ERNEST_PATH=$1

d=$(date +'%Y%m%d_%H%M%S')
LOGFILE=$ERNEST_PATH/cronjobs/logs/log_$d

echo "-- edgar data ingestion --"
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-index.sh >> $LOGFILE 
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-forms.sh >> $LOGFILE 

echo "-- enrich edgar aggregation indices --"
cd $ERNEST_PATH/cronjobs/ && bash run-ownership.sh >> $LOGFILE 
cd $ERNEST_PATH/cronjobs/ && bash run-symbology.sh >> $LOGFILE 

echo "-- run otc scrapes --"
cd $ERNEST_PATH/cronjobs/ && bash run-otc-scrape.sh >> $LOGFILE 
cd $ERNEST_PATH/cronjobs/ && bash run-finra-scrapes.sh >> $LOGFILE 

# echo "-- run halts scrape & merge --"
# cd $ERNEST_PATH/cronjobs/ && bash run-halts-process.sh >> $LOGFILE 

echo "-- enrich otc, symbology & ownership --"
cd $ERNEST_PATH/cronjobs/ && bash run-add-otc-flag.sh >> $LOGFILE 
cd $ERNEST_PATH/cronjobs/ && bash run-sic-enrich.sh >> $LOGFILE 

# echo "-- enrich terminal nodes --"
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-terminal-nodes.sh >> $LOGFILE 

# echo "-- get new xbrl sub docs if available --" 
# cd $ERNEST_PATH/cronjobs/ && bash run-xbrl-submission-scrape.sh >> $LOGFILE 

# echo "-- update delinquency --"
# cd $ERNEST_PATH/cronjobs/ && bash run-build-delinquency.sh >> $LOGFILE 

# echo "-- get new xbrl rss docs --" 
# cd $ERNEST_PATH/cronjobs/ && bash run-xbrl.sh >> $LOGFILE  # error

# echo "-- compute delinquency --"
# cd $ERNEST_PATH/cronjobs/ && bash run-compute-delinquency.sh >> $LOGFILE 

# echo "-- compute fye graph, normalize xbrl financials values --"
# cd $ERNEST_PATH/cronjobs/ && bash run-compute-fye-graph.sh >> $LOGFILE 
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-aqfs-fye.sh >> $LOGFILE  # error
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-normalize-financials.sh >> $LOGFILE # error

# echo "-- update nt filings index and enrich financials documents --"
# cd $ERNEST_PATH/cronjobs/ && bash run-build-nt-filings.sh >> $LOGFILE # error
# cd $ERNEST_PATH/cronjobs/ && bash run-enrich-ntfilings-period.sh >> $LOGFILE # error
# cd $ERNEST_PATH/cronjobs/ && bash run-add-nt-filings-tag.sh >> $LOGFILE # error

echo "-- scrape and enrich touts --"
cd $ERNEST_PATH/cronjobs/ && bash run-scrape-touts.sh >> $LOGFILE 
cd $ERNEST_PATH/cronjobs/ && bash run-enrich-touts.sh >> $LOGFILE 

# echo "-- enrich crowdsar data & update pv index --"
cd $ERNEST_PATH/investor-forums/cronjobs/ && bash run-daily.sh >> $LOGFILE 

# echo "-- aggregations --"
# cd $ERNEST_PATH/aggregations && bash run-all.sh
