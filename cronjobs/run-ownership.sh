#!/bin/bash

# Takes new documents from the forms index and incorporates them into the ownership 
# aggregation index
# 
# Takes arguments: 
#  --last-week : limits the scope of the enrichment to documents from the week before
# 
# Run daily to ensure the index is current


echo 'run-ownership.sh'

SPARK_HOME=/srv/software/spark-1.6.1

$SPARK_HOME/bin/spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar \
     ../enrich/compute-ownership-graph.py --last-week
