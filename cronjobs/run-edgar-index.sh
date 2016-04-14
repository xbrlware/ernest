#!/bin/bash

echo "run-edgar-index.sh"
python $1/scrape-edgar-index.py --most-recent --config-path $1/config.json
