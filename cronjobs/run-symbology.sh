#!/bin/bash

echo 'run-symbology.sh'

SPARK_HOME=/srv/software/spark-1.6.1

$SPARK_HOME/bin/spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar \
    ../enrich/compute-index2symbology.py --last-week


$SPARK_HOME/bin/spark-submit \
    --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar \
    ../enrich/compute-ownership2symbology.py --last-week