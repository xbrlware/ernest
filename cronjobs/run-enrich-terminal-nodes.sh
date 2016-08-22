#!/bin/bash

# Enrich terminal nodes for single neighbor entries in ownership index
# 
# Takes argument --most-recent to only enrich new documents
# 
# Run each day to ensure index is current

echo "run-enrich-terminal-nodes"

SPARK_HOME=/srv/software/spark-1.6.1
SPARK_CMD="$SPARK_HOME/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar "

$SPARK_CMD ../enrich/enrich-terminal-nodes.py --most-recent --issuer --owner