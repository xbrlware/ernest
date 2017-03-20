#!/bin/bash

# Scripts here used to download json data from historical finra archives for otc companies
# 
# Enrich script used to convert unix timestamps to useable dates in the delinquency
# and directory indices.
#
# Takes arguments: 
#  --directory : if "directory" then the script will scrape the running list of all otc 
#                   companies published and updated on the finra site.
#                if "delinquency" then the script will scrape the newly released list of 
#                   delinquent otc companies. 
# 
# Run daily to ensure otc data is current with available finra data

now=$(date)
d=$(date +'%Y%m%d_%H%M%S')

python2.7 ../scrape/scrape-finra.py \
        --directory="directory" \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d" \
        --date="$now"

python2.7 ../scrape/scrape-finra.py \
        --directory="delinquency" \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d" \
        --date="$now"
