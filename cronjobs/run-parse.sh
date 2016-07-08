#!/bin/bash

cd /home/ubuntu/sec/filings__$1__$2

while ls | grep -q .
do  
  echo 'parsing'
  timeout 600s Rscript /home/ubuntu/ernest/scrape/xbrl-parse.R $1 $2; python /home/ubuntu/ernest/scrape/xbrl-ingest.py --year=$1 --month=$2
done

cd /home/ubuntu

rm -r /home/ubuntu/sec/filings__$1__$2
rm -r /home/ubuntu/sec/parsed_min__$1__$2
rm -r /home/ubuntu/sec/unzipped__$1__$2


