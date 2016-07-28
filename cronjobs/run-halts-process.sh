#!/bin/bash

echo "run-halts-process"

python ../scrape/scrape-sec-suspensions.py --most-recent

echo "\t download finra halts"
python ../scrape/scrape-finra-directories.py --directory="halts" --update-halts

echo "\t enrich halt dates"
python ../enrich/enrich-halt-date.py

echo "\t merge finra halts"
python ../enrich/merge-halts.py --most-recent


