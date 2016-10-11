#!/bin/bash

# Scripts here used to download json data from historical finra archives for otc companies
# 
# Enrich script used to convert unix timestamps to useable dates in the delinquency
# and directory indices.
#
# Takes arguments: 
#  --directory : if "directory" then the script will scrape the running list of all otc 
#                   companies published and updated on the finra site.
#                if "delinwuency" then the script will scrape the newly released list of 
#                   delinwuent otc companies. 
# 
# Run daily to ensure otc data is current with available finra data

echo "run-finra-scrapes.sh"

IN=$(curl -XGET 'localhost:9205/ernest_otc_directory_cat/_count?pretty' | jq '.count') 

echo "\t directory"
python ../scrape/scrape-finra-directories.py --directory="directory"

OUT=$(curl -XGET 'localhost:9205/ernest_otc_directory_cat/_count?pretty' | jq '.count') 
now=$(date)
index="ernest-otc-directory-cat"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 


IN=$(curl -XGET 'localhost:9205/ernest_otc_delinquency_cat/_count?pretty' | jq '.count') 

echo "\t delinquency"
python ../scrape/scrape-finra-directories.py --directory="delinquency"

OUT=$(curl -XGET 'localhost:9205/ernest_otc_delinquency_cat/_count?pretty' | jq '.count') 
now=$(date)
index="ernest-otc-delinquency-cat"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 

echo "\t enrich dates"
python ../enrich/enrich-finra-dates.py --directory="directory"
python ../enrich/enrich-finra-dates.py --directory="delinquency"
