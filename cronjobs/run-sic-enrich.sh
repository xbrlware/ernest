#!/bin/bash

# Script adds text description of SIC code accompanying each entry in symbology and 
# ownerhsip indices
# 
# Takes argument: 
#  --index : name of index to enrich with SIC description (symbology or ownership)
# 
# Run daily to ensure all summary data for companies and owners is up to date where 
# that information is available

echo 'run-sic-enrich'

IN=$(curl -XGET localhost:9205/ernest_symbology_v2/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.sic_lab"
        }
      }
    }
  }
}' | jq '.count')

echo '\t adding sic info to symbology documents'
python ../enrich/enrich-add-sic-descs.py --index='symbology'

OUT=$(curl -XGET localhost:9205/ernest_symbology_v2/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.sic_lab"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-symbology-v2-enrich-sic"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 


IN=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.sic_lab"
        }
      }
    }
  }
}' | jq '.count')

echo '\t adding sic info to ownership documents'
python ../enrich/enrich-add-sic-descs.py --index='ownership'

OUT=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{ 
  "query" : { 
    "filtered" : { 
      "filter" : { 
        "missing" : { 
          "field" : "__meta__.sic_lab"
        }
      }
    }
  }
}' | jq '.count')

now=$(date)
index="ernest-ownership-cat-enrich-sic"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 

