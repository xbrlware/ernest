#!/bin/bash

# Ingest new nt filings documents into ernest_nt_filings index
# 
# Takes argument --most-recent to only grab new filings
# 
# Run each day to ensure index is current

echo "run-build-nt-filings"
python ../enrich/build-nt-filings.py --most-recent
