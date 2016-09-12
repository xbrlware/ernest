#!/bin/bash

# Takes new documents from the forms index and incorporates them into the ownership 
# aggregation index
# 
# Takes arguments: 
#  --last-week : limits the scope of the enrichment to documents from the week before
# 
# Run daily to ensure the index is current

IN=$(curl -XGET 'localhost:9205/ernest_ownership_cat/_count?pretty' | jq '.count') 

echo 'run-ownership'

SPARK_HOME=/srv/software/spark-1.6.1
SPARK_CMD="$SPARK_HOME/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"

$SPARK_CMD ../enrich/compute-ownership-graph.py --last-week

OUT=$(curl -XGET 'localhost:9205/ernest_ownership_cat/_count?pretty' | jq '.count') 

now=$(date)

index="ernest-ownership-cat"

python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 