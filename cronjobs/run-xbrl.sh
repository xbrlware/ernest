#!/bin/bash
echo $1
if [$1 != ''] ; then
    echo 'updating'
    myyear=`date +'%Y'`
    mymonth=`date +'%m'`
    mymonth=${mymonth#0}
    echo $mymonth
    echo $myyear
    sh xbrl-wrapper.sh $myyear $mymonth
else
    for i in `seq $2 12`;
    do
        echo 'running for month'
        echo $i
        echo 'sh xbrl-wrapper.sh $1 $i'
    done  
fi