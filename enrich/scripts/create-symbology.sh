#!/bin/bash

'''
    Construct symbology index from
        edgar_indices
        forms 3 and 4
'''


SPARK_CMD="/srv/software/spark-1.6.1/bin/spark-submit --master local[3] --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar"


echo '-- setting mappings --'
python ../helpers/add-cat-mappings.py --index symbology --doc-types entry

echo '-- symbology from ownership --'
$SPARK_CMD compute-ownership2symbology.py  --from-scratch

echo '-- symbology from index --'
$SPARK_CMD compute-index2symbology.py  --from-scratch

echo '-- adding otc flag --'
python enrich-add-otc-flag.py --index symbology --field-name ticker

echo '-- adding sic label --'
python enrich-add-sic-descs.py --index symbology 