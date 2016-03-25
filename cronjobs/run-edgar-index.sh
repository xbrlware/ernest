#!/bin/bash

echo "starting up update indices..."

path=/home/emmett/ernest

echo "downloading & uploading this quarter's index..."
python $path/scrape-edgar-index.py --most-recent --config-path='/home/emmett/ernest/config.json'

echo "finishing up..."
