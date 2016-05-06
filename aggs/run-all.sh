#!/bin/bash

CONFIG_PATH=../config.json
SPARK_CMD="spark-submit --master "local[*]" --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"

$SPARK_CMD aggregate-searchterms.py --config-path $CONFIG_PATH
$SPARK_CMD aggregate-symbology.py   --config-path $CONFIG_PATH
$SPARK_CMD aggregate-delinquency.py --config-path $CONFIG_PATH
