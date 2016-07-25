#!/bin/bash

# Uses edgar FTP to download ownership documents to forms index
# 
# Takes arguments: 
#  --back-fill : try and download all documents that havent been tried or have failed
#  --start-date: start date for back fill ingest
#  --section   : header, body or both
#  --form-types: type of filing to download 
# 
# Run each day after the edgar index script has been populated with new filings

echo 'run-edgar-forms.sh'

python ../scrape/scrape-edgar-forms.py --back-fill \
    --start-date='2010-01-01' \
    --section=both \
    --form-types=3,4 
