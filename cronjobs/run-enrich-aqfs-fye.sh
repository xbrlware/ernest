#!/bin/bash

# Enrich aqfs documents with FYE from xbrl submissions & aggregation index
# 
# Run each day to ensure index is current

echo "run-enrich-aqfs-fye"
python ../enrich/enrich-aqfs-fye.py --sub --function
