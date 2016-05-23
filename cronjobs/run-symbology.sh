#!/bin/bash

echo 'run-symbology.sh'

SPARK_HOME=/srv/software/spark-1.6.1

$SPARK_HOME/bin/spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar \
    ../enrich/compute-symbology.py --last-week
