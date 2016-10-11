#!/bin/bash

# Normalize time scale for financials values 
# 
# Takes argument --most-recent to only run for documents that havent been scaled
# 
# Run each day to ensure index is current

IN=$(curl -XGET localhost:9205/ernest_aq_forms/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.financials.scaled"
        }
      }
    }
  }
}' | jq '.count')

echo "run-enrich-normalize-financials"
python ../enrich/enrich-normalize-financials.py --most-recent

OUT=$(curl -XGET localhost:9205/ernest_aq_forms/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.financials.scaled"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-aq-forms-scale-financials"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 