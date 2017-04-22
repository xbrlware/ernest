#!/bin/bash

# Enrich touts with cik/ticker resolution
# 
# Takes argument --most-recent to only enrich new documents
# 
# Run each day to ensure index is current

echo "run-enrich-touts"
python ../enrich/enrich-touts.py --most-recent