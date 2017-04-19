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

#  --xbrl-parse.R: 
#    This script uses an R library called XBRL to unzip, parse and build a csv file for 
#    each document in the 'filings_year_month' directory. When each document is parsed
#    the original zip archive and the raw unzipped submission are deleted to conserve 
#    space. This process runs for ten minutes, and then the ingestion script is set off. 
#    
#  --xbrl-ingest.py: 
#    This script takes each parsed csv document output from the xbrl-parse.R and writes 
#    them into the 'ernest_xbrl_rss' index. Before they are ingested, each document is 
#    cleaned, dates are coerced into acceptable format, duplicate tags are eliminated 
#    and the financial tags are reduced to a predetermined set. Once every document has
#    has been tried, all directories are cleared and the process begins again for another 
#    chunk of filings. 

# scrape
d=$(date +'%Y%m%d_%H%M%S')
python2.7 /home/ubuntu/ernest/scrape/xbrl-download.py \
        --year=$1 \
        --month=$2 \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"

Rscript /home/ubuntu/ernest/scrape/xbrl-parse.R $1 $2 >> "/home/ubuntu/ernest/cronjobs/logs/log_$d"

python2.7 /home/ubuntu/ernest/scrape/xbrl-ingest.py \
        --year=$1 \
        --month=$2 \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"

# enrich xbrl documents
python2.7 /home/ubuntu/ernest/enrich/xbrl-rss-enrich.py --year=$1 --month=$2

# interpolating xbrl documents
python2.7 /home/ubuntu/ernest/enrich/xbrl-rss-interpolation.py
