#!/bin/bash

echo 'run-ownership.sh'

SPARK_HOME=/srv/software/spark-1.6.1

$SPARK_HOME/bin/spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar \
     ../enrich/compute-ownership-graph.py --last-week --config-path $1
