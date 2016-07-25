#!/bin/bash

# Shell wrapper for xbrl rss shell scripts
# 
# Calls xbrl-wrapper.sh with year and month arguments 
# 
# If no argument is supplied to the script; xbrl-wrapper.sh is run for the current 
# month and year
# 
# If a year argument is supplied; xbrl-wrapper.sh is run for each month of the year 
# supplied as the argument
# 
# Run daily to ensure xbrl rss data is complete as new documents are released each 
# working day

echo $1
if [$1 != ''] ; then
    echo 'updating'
    myyear=`date +'%Y'`
    mymonth=`date +'%m'`
    echo $mymonth
    echo $myyear
    sh xbrl-wrapper.sh $myyear ${mymonth#0}
else
    for i in `seq $2 12`;
    do
        echo 'running for month'
        echo $i
        echo 'sh xbrl-wrapper.sh $1 $i'
    done  
fi