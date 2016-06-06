#!/bin/bash

CONFIG_PATH=../config.json
SPARK_CMD="spark-submit --master "local[*]" --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"

# echo '-- setting mapping --'
# python set-agg-mapping.py --config-path $CONFIG_PATH

echo '-- running aggs --'
$SPARK_CMD aggregate-searchterms.py --config-path $CONFIG_PATH
$SPARK_CMD aggregate-symbology.py   --config-path $CONFIG_PATH
$SPARK_CMD aggregate-delinquency.py --config-path $CONFIG_PATH
$SPARK_CMD aggregate-otc-neighbors.py --config-path $CONFIG_PATH
