#!/bin/bash

echo "run-finra-scrapes.sh"

echo "directory"
python ../scrape/scrape-finra-directories.py --directory='directory'

echo "halts"
python ../scrape/scrape-finra-directories.py --directory='halts'

echo "delinquency"
python ../scrape/scrape-finra-directories.py --directory='delinquency'