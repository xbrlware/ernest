#!/bin/bash

echo 'run-symbology.sh'

spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.2.0.jar \
    ../compute-symbology.py --last-week --config-path $1