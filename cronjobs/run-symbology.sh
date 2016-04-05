#!/bin/bash

echo 'updating symbology index'

path=/home/ubuntu/ernest

/srv/software/spark-1.6.1/bin/spark-submit --jars /srv/software/spark-1.6.1/jars/elasticsearch-hadoop-2.2.0.jar $path/compute-symbology.py --last-week --config-path='/home/ubuntu/ernest/config.json'