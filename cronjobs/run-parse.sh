#!/bin/bash

# This shell runs two scripts that parse and ingest the xbrl rss documents downloaded 
# for a given year & month by the 'xbrl-download.py' script. 
# 
# Calls scripts: 
# 
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
# 
# NOTE: The proces is organized in this way to conserve space. The unzipped xbrl documents 
# can be very large, as can the csv output from the R parsing utility. Structuring it in 
# this way limits the number of unzipped filings saved in memory to 1, and limits the 
# number of csv documents saved in memory by clearing the directory and ingesting every ten
# minutes. 
# 
# Script should be run every day for newly downloaded xbrl rss filings.


cd /home/ubuntu/sec/filings__$1__$2

while ls | grep -q .
do  
  echo 'parsing'
  timeout 600s Rscript /home/ubuntu/ernest/scrape/xbrl-parse2.R $1 $2; python /home/ubuntu/ernest/scrape/xbrl-ingest.py --year=$1 --month=$2
  rm -r /home/ubuntu/filings__$1__$2/xbrl.Cache
done

cd /home/ubuntu

rm -r /home/ubuntu/sec/filings__$1__$2
rm -r /home/ubuntu/sec/parsed_min__$1__$2
rm -r /home/ubuntu/sec/unzipped__$1__$2


