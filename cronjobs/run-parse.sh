#!/bin/bash

cd /home/ubuntu/sec/2011/07

while ls | grep -q .
do  
  echo 'parsing'
  timeout 300s Rscript /home/ubuntu/ernest/scrape/dev-xbrl-parse.R '2011' '07'
done