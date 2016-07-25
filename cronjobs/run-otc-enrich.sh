#!/bin/bash

echo "run-otc-enrich"

SPARK_HOME=/srv/software/spark-1.6.1
SPARK_CMD="$SPARK_HOME/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"

$SPARK_CMD ../enrich/enrich-otc.py
