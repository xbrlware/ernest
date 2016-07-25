#!/bin/bash

# Scripts here used to download json data from historical finra archives for otc companies
# 
# Enrich script used to convert unix timestamps to useable dates in the delinquency
# and directory indices.
#
# Takes arguments: 
#  --directory : if 'directory' then the script will scrape the running list of all otc 
#                   companies published and updated on the finra site.
#                if 'delinwuency' then the script will scrape the newly released list of 
#                   delinwuent otc companies. 
# 
# Run daily to ensure otc data is current with available finra data

echo "run-finra-scrapes.sh"

echo "directory"
python ../scrape/scrape-finra-directories.py --directory='directory'

echo "delinquency"
python ../scrape/scrape-finra-directories.py --directory='delinquency'


echo 'enrich dates'

python ../enrich/enrich-finra-dates.py --directory='directory'
python ../enrich/enrich-finra-dates.py --directory='delinquency'