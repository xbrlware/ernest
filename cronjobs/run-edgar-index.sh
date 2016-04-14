#!/bin/bash

echo "run-edgar-index.sh"
python ../scrape-edgar-index.py --most-recent --config-path $1
