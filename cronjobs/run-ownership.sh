#!/bin/bash

echo 'updating ownership index'

path=/home/emmett/ernest

/home/ubuntu/spark/bin/pyspark --master "local[*]" --jars /home/ubuntu/spark/jars/elasticsearch-hadoop-2.1.0.rc1.jar $path/compute-ownership-graph.py --last-week --config-path= '/home/emmett/ernest/config.json'