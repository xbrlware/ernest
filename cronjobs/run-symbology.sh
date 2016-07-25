#!/bin/bash

# Takes documents from indices edgar_index_cat and ernest_forms_cat and incorporates 
# them into the symbology aggregation index. 
# 
# The first script takes all non ownership filings from edgar_index_cat and incorporates 
# them into the symbology aggregation. 
# 
# The second takes ownership documents from ernest_forms_cat and incorprates them into the 
# symbology aggregation index 
# 
# Takes arguments: 
#  --last-week : limits the scope of the enrichment to filings from the previous week
# 
# Run daily to ensure data in the symbology index is current

echo 'run-symbology'

SPARK_HOME=/srv/software/spark-1.6.1
SPARK_CMD="$SPARK_HOME/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar "

$SPARK_CMD ../enrich/compute-index2symbology.py --last-week
$SPARK_CMD ../enrich/compute-ownership2symbology.py --last-week
