#!/bin/bash

echo 'updating ownership index'

path=/home/ubuntu/ernest

/srv/software/spark-1.6.1/bin/spark-submit --jars /srv/software/spark-1.6.1/jars/elasticsearch-hadoop-2.2.0.jar $path/compute-ownership-graph.py --from-scratch --config-path='/home/ubuntu/ernest/config.json'