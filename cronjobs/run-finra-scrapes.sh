#!/bin/bash

echo "run-finra-scrapes.sh"
echo "directory"
python ../scrape/scrape-otc.py --directory='otc'
echo "halts"
python ../scrape/scrape-otc.py --directory='halts'
echo "delinquency"
python ../scrape/scrape-otc.py --directory='delinquency'