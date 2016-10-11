#!/bin/bash

# This script adds and 'is_otc' flag to the symbology and ownership aggregation indices
# 
# Takes arguments: 
#  --field-name : the name of the field with the ticker in it for the given index (sym or own)
#  --index      : the name of the index to be enriched with flag (symbology or ownership)
# 
# Run daily to ensure new otc data is included in aggregation indices

echo "running enrich-add-otc-flag"

IN=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.is_otc"
        }
      }
    }
  }
}' | jq '.count')

python ../enrich/enrich-add-otc-flag.py --index ownership --field-name issuerTradingSymbol

OUT=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.is_otc"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-ownership-cat-enrich"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 


IN=$(curl -XGET localhost:9205/ernest_symbology_v2/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.is_otc"
        }
      }
    }
  }
}' | jq '.count')

python ../enrich/enrich-add-otc-flag.py --index symbology --field-name ticker


OUT=$(curl -XGET localhost:9205/ernest_symbology_v2/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.is_otc"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-symbology-v2-enrich"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 




