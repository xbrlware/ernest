#!/bin/bash

# run-all.sh
# Run all aggregations
# 
# Ex:
#   ./run-all.sh ../config.json

echo 'run-all (aggregations)'


CONFIG_PATH=$1
SPARK_CMD="spark-submit --master "local[*]" --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"

echo '\t setting mappings'
python ./set-mappings.py --config-path $CONFIG_PATH

echo '\t searchterms'
$SPARK_CMD aggregate-searchterms.py --config-path $CONFIG_PATH

echo '\t symbology'
$SPARK_CMD aggregate-symbology.py   --config-path $CONFIG_PATH

echo '\t delinquency'
$SPARK_CMD aggregate-delinquency.py --config-path $CONFIG_PATH

echo '\t financials'
$SPARK_CMD aggregate-financials.py --config-path $CONFIG_PATH

echo '\t 2-hop-otc'
$SPARK_CMD aggregate-2-hop-otc.py --config-path $CONFIG_PATH

echo '\t suspensions'
$SPARK_CMD aggregate-suspensions.py --config-path $CONFIG_PATH

echo '\t crowdsar'
$SPARK_CMD aggregate-crowdsar.py --config-path $CONFIG_PATH
