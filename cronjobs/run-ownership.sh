#!/bin/bash

echo 'run-ownership.sh'

spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.2.0.jar \
     ../compute-ownership-graph.py --last-week --config-path $1
