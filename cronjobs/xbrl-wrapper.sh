#!/bin/bash

# Script runs each step in the xbrl rss ingestion process for a given month and year
# 
# Month and year arguments are supplied by the run-xbrl.sh script that calls this process
# 
# Calls scripts: 
#  --xbrl-download.py: 
#    This script downloads xbrl rss documents from the sec rss feed and saves them into 
#    zip archives in a temporary filings directory 'filings__year__month'
# 
#  --run-parse.sh: 
#    This shell calls an R script utility unzips the files and saves them to a temp directory 
#    'unzipped_year_month'. The utility then parses the fiings and write them to another 
#    temporary directory 'parsed_min_year_month'
#    This part of the process runs once for each document, such that each individual filing is 
#    parsed, the raw zip archive downloaded from the sec feed is deleted and the unzipped 
#    filing is deleted.
# 
#    After 10 minutes of parsing, the script calls a python ingestion script 'xbrl-ingest.py' 
#    that cleans the csv files and writes them into the 'ernest_xbrl_rss' index. Once all documents
#    in the 'parsed_min_year_month' directory have been ingested, all temp deirectories are deleted 
#    and the process runs again. This continues untill there are no longer any filings documents 
#    in the download directory 'filings_year_month', then deletes all directories involved in the 
#    process
# 
#  --xbrl-rss-enrich.py: 
#    This script takes the newly ingested rss documents from 'ernest_xbrl_rss', cleans them, 
#    limits them to a set of predetermined tags, and appends the information to the 'ernest_aq_forms' 
#    index which contains an entry for each 10-K or 10-Q document available in the 'edgar_index_cat'
#    index. 
# 
#  --xbrl-rss-interpolation: 
#    This script runs for each document in the 'ernest_aq_forms' index after it has been enriched 
#    with the financials from 'ernest_xbrl_rss'. The script uses basic balance sheet identities to 
#    interpolate financial values from existing information when they are not provided explicity.

# python2.7 ../scrape/xbrl-download.py \
#        --year=$1 \
#        --month=$2 \
#        --log-file="/home/ubuntu/ernest/cronjobs/logs/xbrl.log"
#
echo "\t parse & ingest xbrl documents"
bash ./run-parse.sh $1 $2

echo "\t enrich xbrl documents"
python2.7 ../enrich/xbrl-rss-enrich.py --year=$1 --month=$2

echo "\t interpolating xbrl documents"
python2.7 ../enrich/xbrl-rss-interpolation.py
