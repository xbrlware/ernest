#!/bin/bash
for i in `seq 7 12`;
do
    echo 'running for month'
    echo $i
    sh xbrl-wrapper.sh $1 $i
done  