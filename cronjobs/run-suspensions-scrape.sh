#!/bin/bash

echo "run-suspensions-scrape.sh"
python ../scrape/scrape-sec-suspensions.py --most-recent --config-path $1