#!/bin/bash

# Script downloads all annual and quarterly filings from the edgar aqfs database
#
# Only the submission files are downloaded to build a directory of expected filings 
# to verify against the rss documents. 
# 
# These documents are also used as the primary source of filer status for the delinquency 
# index
# 
# Takes arguments: 
#  --most-recent : limits the scope of documents to be ingested
# 
# Run at the end of each quarter to get newly published submission documents 


IN=$(curl -XGET 'localhost:9205/xbrl_submissions_cat/_count?pretty' | jq '.count') 

echo "run-xbrl-submissions-scrape"
python ../scrape/scrape-xbrl-submissions.py --most-recent


OUT=$(curl -XGET 'localhost:9205/xbrl_submissions_cat/_count?pretty' | jq '.count') 

now=$(date)

index="xbrl-submissions-cat"

python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 