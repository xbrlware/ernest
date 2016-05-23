#!/bin/bash

echo "run-finra-scrapes.sh"
python ../scrape/scrape-otc.py --directory='otc'
python ../scrape/scrape-otc.py --directory='halts'
python ../scrape/scrape-otc.py --directory='delinquency'