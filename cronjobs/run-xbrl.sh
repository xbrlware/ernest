#!/bin/bash
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