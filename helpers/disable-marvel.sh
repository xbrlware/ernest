#!/bin/bash

ES=localhost:9205

curl -XPUT "http://$ES/_cluster/settings" -d'
{
  "persistent": {
    "marvel.agent.interval": "-1"
  }
}'

