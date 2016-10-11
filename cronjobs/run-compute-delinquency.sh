#!/bin/bash

# Script is used to compute delinwuency from the information in the ernest_aq_forms index
# 
# The period and filer status are used to compute a deadline for the filing and then 
# the submission date is used to evaluate delinwuency
# 
# Run daily to ensure data in ernest_aq_forms is current with available information 


IN=$(curl -XGET localhost:9205/ernest_aq_forms/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "_enrich.is_late"
        }
      }
    }
  }
}' | jq '.count')

echo "run-compute-delinquency"
python ../enrich/compute-delinquency.py

OUT=$(curl -XGET localhost:9205/ernest_aq_forms/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "_enrich.is_late"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-aq-forms-is-late"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 