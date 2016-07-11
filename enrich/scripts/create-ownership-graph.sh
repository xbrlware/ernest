#!/bin/bash

'''
    Construct ownership index from
        forms 3 and 4
'''


SPARK_CMD="/srv/software/spark-1.6.1/bin/spark-submit --master local[3] --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"

echo '-- setting mappings --'
python ../helpers/add-cat-mappings.py --index ownership

# run compute-ownership-graph.py
echo '-- computing ownership graph --'
python compute-ownership-graph.py  --from-scratch

echo '-- adding otc flag --'
python enrich-add-otc-flag.py --index ownership --field-name issuerTradingSymbol

echo '-- adding sic label --'
python enrich-add-sic-descs.py --index ownership 