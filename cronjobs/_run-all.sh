#!/bin/bash

# Add entry to `crontab` like
# 0 1 * * * /home/ubuntu/ernest/crontabs/_run-all.sh /home/ubuntu/ernest
# 

ERNEST_PATH=$1

echo "-- edgar data ingestion --"
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-index.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-edgar-forms.sh 

echo "-- enrich edgar aggregation indices --"
cd $ERNEST_PATH/cronjobs/ && bash run-ownership.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-symbology.sh 

echo "-- run otc scrapes --"
cd $ERNEST_PATH/cronjobs/ && bash run-otc-scrape.sh 
cd $ERNEST_PATH/cronjobs/ && bash run-finra-scrapes.sh

echo "-- run halts scrape & merge --"
cd $ERNEST_PATH/cronjobs/ && bash run-halts-process.sh

echo "-- enrich otc, symbology & ownership --"
cd $ERNEST_PATH/cronjobs/ && bash run-add-otc-flag.sh
cd $ERNEST_PATH/cronjobs/ && bash run-sic-enrich.sh

echo "-- enrich terminal nodes --"
cd $ERNEST_PATH/cronjobs/ && bash run-enrich-terminal-nodes.sh

echo "-- get new xbrl sub docs if available --" 
cd $ERNEST_PATH/cronjobs/ && bash run-xbrl-submission-scrape.sh

echo "-- update delinquency --"
cd $ERNEST_PATH/cronjobs/ && bash run-build-delinquency.sh

echo "-- get new xbrl rss docs --" 
cd $ERNEST_PATH/cronjobs/ && bash run-xbrl.sh

echo "-- compute delinquency --"
cd $ERNEST_PATH/cronjobs/ && bash run-compute-delinquency.sh

echo "-- compute fye graph, normalize xbrl financials values --"
cd $ERNEST_PATH/cronjobs/ && bash run-compute-fye-graph.sh
cd $ERNEST_PATH/cronjobs/ && bash run-enrich-aqfs-fye.sh
cd $ERNEST_PATH/cronjobs/ && bash run-enrich-normalize-financials.sh

echo "-- update nt filings index and enrich financials documents --"
cd $ERNEST_PATH/cronjobs/ && bash run-build-nt-filings.sh
cd $ERNEST_PATH/cronjobs/ && bash run-enrich-ntfilings-period.sh
cd $ERNEST_PATH/cronjobs/ && bash run-add-nt-filings-tag.sh

echo "-- scrape and enrich touts --"
cd $ERNEST_PATH/cronjobs/ && bash run-scrape-touts.sh
cd $ERNEST_PATH/cronjobs/ && bash run-enrich-touts.sh

echo "-- enrich crowdsar data & update pv index --"
cd $ERNEST_PATH/investor-forums/cronjobs/ && bash run-daily.sh

# echo "-- aggregations --"
# cd $ERNEST_PATH/aggregations && bash run-all.sh
