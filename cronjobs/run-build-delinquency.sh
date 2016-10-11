#!/bin/bash

# Script takes 10-K and 10-Q documents from the edgar_index_cat index and writes them 
# to the ernest_aq_forms index. 
# 
# The script also then enriches those documents with filer status from the xbrl-submissions 
# index; and with the period of filing from the xml documents online
# 
# Takes arguments: 
#  --status : gets filer status from xbrl-submissions or from the most recent document with that information
#  --period : get the period of the filing from the xml tree on edgar for the filing
# 
# Run daily to ensure data is current with available edgar financials

echo "run-build-delinquency"

IN=$(curl -XGET 'localhost:9205/ernest_aq_forms/_count?pretty' | jq '.count') 

echo "\t getting filer status"
python ../scrape/build-delinquency.py --update --status

echo "\t getting filing deadlines"
python ../scrape/build-delinquency.py --update --period

OUT=$(curl -XGET 'localhost:9205/ernest_aq_forms/_count?pretty' | jq '.count') 

now=$(date)

index="ernest-aq-forms-delinquency"

python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 
