#!/bin/bash

# Add nt filing exists tag for documents in financials index
# 
# Takes argument --most-recent to run for new nt documents
# 
# Run each day to ensure index is current

echo "run-add-nt-filings-tag"
python ../enrich/enrich-add-nt-filings-tag.py --most-recent
