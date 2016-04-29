#!/bin/bash

echo "starting up update indices..."

path=/home/ubuntu/ernest

echo "downloading & uploading this quarter's index..."
python $path/scrape-edgar-index.py --most-recent --config-path='/home/ubuntu/ernest/config.json'

echo "finishing up..."
