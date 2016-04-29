import json
import argparse
from elasticsearch import Elasticsearch

# --
# CLI

parser = argparse.ArgumentParser(description='grab_new_filings')
parser.add_argument("--index", type=str)
parser.add_argument("--alias", type=str)
parser.add_argument("--config-path", type=str, action='store', default='../config.json')
args = parser.parse_args()

config = json.load(open(args.config_path))

# --
# Connections
client = Elasticsearch([{
    "host" : config['es']['host'], 
    "port" : config['es']['port']
}])

print 'adding alias \t\t %s \t\t -> \t\t %s' % (args.alias, args.index)
client.indices.put_alias(index=args.index, name=args.alias)