#!/bin/bash

# run-all.sh
# Run all aggregations
# 
# Ex:
#   ./run-all.sh ../config.json

CONFIG_PATH=$1

python2.7 set-mappings.py --config-path $CONFIG_PATH

python2.7 aggregate-searchterms.py --config-path $CONFIG_PATH

python2.7 aggregate-symbology.py   --config-path $CONFIG_PATH

python2.7 aggregate-delinquency.py --config-path $CONFIG_PATH

python2.7 aggregate-financials.py --config-path $CONFIG_PATH

python2.7 aggregate-2-hop-otc.py --config-path $CONFIG_PATH

python2.7 aggregate-suspensions.py --config-path $CONFIG_PATH

# $SPARK_CMD aggregate-crowdsar.py --config-path $CONFIG_PATH
