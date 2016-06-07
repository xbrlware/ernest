#!/bin/bash

SPARK_CMD="/srv/software/spark-1.6.1/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"


echo '-- setting mappings --'
python ../helpers/add-cat-mappings.py \
  --index ernest_symbology_v2 \
  --doc-types forms_3_4,index 

echo '-- symbology from ownership --'
$SPARK_CMD compute-ownership2symbology.py  --last-week

echo '-- symbology from index --'
$SPARK_CMD compute-index2symbology.py  --last-week
