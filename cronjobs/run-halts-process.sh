#!/bin/bash

# Scrape and ingest new trading suspensions from the SEC and FINRA otc json page
# 
# This shell process includes four scripts: 
# 
# scrape-sec-suspensions: 
#   --scrape and ingest new SEC suspensions
# scrape-finra-directories: 
#   --scrape and ingest new trading halts from the FINRA otc json page
# enrich-halt-date: 
#   --coerce halt date for otc halts to yyyy-mm-dd from unix timestamp
# merge-halts.py: 
#   --merge otc halts into sec halts index using high level entity resolution criteria
# 
# Run each day to ensure index is current

echo "run-halts-process"

python ../scrape/scrape-sec-suspensions.py --most-recent

echo "\t download finra halts"
python ../scrape/scrape-finra-directories.py --directory="halts" --update-halts

echo "\t enrich halt dates"
python ../enrich/enrich-halt-date.py

echo "\t merge finra halts"
python ../enrich/merge-halts.py --most-recent


