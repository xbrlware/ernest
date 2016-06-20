#!/bin/bash

cd /home/ubuntu/sec/filings

while ls | grep -q .
do  
  echo 'parsing'
  timeout 600s Rscript /home/ubuntu/ernest/scrape/dev-xbrl-parse.R $1 $2 
done