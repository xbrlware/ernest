#!/bin/bash

# Enrich terminal nodes for single neighbor entries in ownership index
# 
# Takes argument --most-recent to only enrich new documents
# 
# Run each day to ensure index is current

echo "run-enrich-terminal-nodes"

IN=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{
        "query" : { 
            "bool" : { 
                "should" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "missing" : { 
                                    "field" : "__meta__.owner_has_one_neighbor"
                                }
                            }
                        }
                    }, 
                    {
                        "match" : { 
                            "__meta__.owner_has_one_neighbor" : true
                        }
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }' | jq '.count')

OUT=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{
        "query" : { 
            "bool" : { 
                "should" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "missing" : { 
                                    "field" : "__meta__.owner_has_one_neighbor"
                                }
                            }
                        }
                    }, 
                    {
                        "match" : { 
                            "__meta__.owner_has_one_neighbor" : true
                        }
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }' | jq '.count')

now=$(date)
index="ernest-ownerhsip-cat-enrich-n-owner"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 



IN=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{
        "query" : { 
            "bool" : { 
                "should" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "missing" : { 
                                    "field" : "__meta__.issuer_has_one_neighbor"
                                }
                            }
                        }
                    }, 
                    {
                        "match" : { 
                            "__meta__.issuer_has_one_neighbor" : true
                        }
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }' | jq '.count')

SPARK_HOME=/srv/software/spark-1.6.1
SPARK_CMD="$SPARK_HOME/bin/spark-submit --jars $SPARK_HOME/jars/elasticsearch-hadoop-2.3.0.jar "

$SPARK_CMD ../enrich/enrich-terminal-nodes.py --most-recent --owner

OUT=$(curl -XGET localhost:9205/ernest_ownership_cat/_count -d '{
        "query" : { 
            "bool" : { 
                "should" : [
                    {
                        "filtered" : { 
                            "filter" : { 
                                "missing" : { 
                                    "field" : "__meta__.issuer_has_one_neighbor"
                                }
                            }
                        }
                    }, 
                    {
                        "match" : { 
                            "__meta__.issuer_has_one_neighbor" : true
                        }
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }' | jq '.count')

now=$(date)
index="ernest-ownerhsip-cat-enrich-n-issuer"
python ../enrich/generic-meta-enrich.py --index="$index" --date="$now" --count-in="$IN" --count-out="$OUT" 
