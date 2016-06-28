#!/bin/bash

echo 'download new sec suspensions'

python ../scrape/scrape-sec-suspensions.py \
    --most-recent


echo 'download finra halts'

python ../scrape/scrape-finra-directories.py \
    --directory = 'halts' \
    --update-halts


echo 'enrich halt dates'

python ../enrich/enrich-halt-date.py


echo 'merge finra halts' \

python ../enrich/merge-halts.py \
    --most-recent


