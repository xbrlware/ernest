#!/bin/bash

# Call like 
#   ./disable-marvel.sh localhost:9205

ES=$1
curl -XPUT "http://$ES/_cluster/settings" -d'
{
  "persistent": {
    "marvel.agent.interval": "-1"
  }
}'

