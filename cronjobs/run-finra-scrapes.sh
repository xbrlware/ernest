#!/bin/bash

echo "run-finra-scrapes.sh"

echo "directory"
python ../scrape/scrape-finra-directories.py --directory='directory'

echo "delinquency"
python ../scrape/scrape-finra-directories.py --directory='delinquency'


echo 'enrich dates'

python ../enrich/enrich-finra-dates.py --directory='directory'
python ../enrich/enrich-finra-dates.py --directory='delinquency'