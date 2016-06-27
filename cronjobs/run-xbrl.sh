#!/bin/bash
for i in `seq 3 12`;
do
    echo 'running for month'
    echo $i
    sh xbrl-wrapper.sh $1 $i
done  