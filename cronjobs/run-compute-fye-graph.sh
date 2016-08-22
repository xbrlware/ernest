#!/bin/bash

# Aggregate FYE schedule for finacials documents
# 
# Takes argument --most-recent to only incorporate new filings into aggregation
# 
# Run each day to ensure index is current

echo "run-compute-fye-graph"

SPARK_HOME=/srv/software/spark-1.6.1
SPARK_CMD="$SPARK_HOME/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar "

$SPARK_CMD ../enrich/compute-fye-graph.py --most-recent
