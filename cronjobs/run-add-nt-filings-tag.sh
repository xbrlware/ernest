#!/bin/bash

# Add nt filing exists tag for documents in financials index
# 
# Takes argument --most-recent to run for new nt documents
# 
# Run each day to ensure index is current

IN=$(curl -XGET localhost:9205/ernest_aq_forms/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "_enrich.NT_exists"
        }
      }
    }
  }
}' | jq '.count')

echo "run-add-nt-filings-tag"
python ../enrich/enrich-add-nt-filings-tag.py --most-recent

OUT=$(curl -XGET localhost:9205/ernest_aq_forms/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "_enrich.NT_exists"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-aq-forms-has-nt"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 