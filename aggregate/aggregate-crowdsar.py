#!/usr/bin/env python

"""
    Compute number of posts and sentiment over time
    ** Recomputes everything every day -- that is less than ideal, but not
       critical **
"""

import json
import argparse
import itertools
from datetime import datetime

from pyspark import SparkContext
sc = SparkContext(appName='aggregate-crowdsar.py')

parser = argparse.ArgumentParser(description='aggregate-crowdsar')
parser.add_argument("--config-path", type=str, action='store')
args = parser.parse_args()

config = json.load(open(args.config_path))

query = {
    "_source": ["__meta__.sym.cik", "__meta__.tri_pred", "time"],
    "query": {
        "filtered": {
            "filter": {
                "and": [
                    {
                        "exists": {
                            "field": "__meta__.tri_pred"
                        }
                    },
                    {
                        "exists": {
                            "field": "__meta__.sym.cik"
                            }
                    },
                    {
                        "exists": {
                            "field": "time"
                        }
                    }
                ]
            }
        }
    }
}

rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf={
        "es.nodes": config['es']['host'],
        "es.port": str(config['es']['port']),
        "es.resource": "%s/%s" % (config['crowdsar']['index'],
                                  config['crowdsar']['_type']),
        "es.query": json.dumps(query)
        }
)


def compute_timeseries(x):
    x = map(lambda x: (
        datetime.strptime(x[0], '%Y-%m-%dT%H:%M:%S').strftime(
            '%Y-%m-%d'), x[1]), x)
    x = sorted(x, key=lambda x: x[0])

    for k, g in itertools.groupby(x, key=lambda x: x[0]):
        g = list(g)
        yield {
            "date": k,
            "n_posts": len(g),
            "tri_pred_neg": sum([p[1]['neg'] for p in g]),
            "tri_pred_neut": sum([p[1]['neut'] for p in g]),
            "tri_pred_pos": sum([p[1]['pos'] for p in g]),
        }


rdd.map(lambda x: x[1])\
    .map(lambda x: (x['__meta__']['sym']['cik'],
                    (x['time'], x['__meta__']['tri_pred'])))\
    .groupByKey()\
    .mapValues(compute_timeseries)\
    .map(lambda x: (x[0], tuple(x[1])))\
    .map(lambda x: ('-', {
        "cik": x[0],
        "crowdsar": x[1],
        "crowdsar_stringified": tuple(
            map(json.dumps, x[1])) if len(x[1]) > 0 else None,
    }))\
    .saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass='org.elasticsearch.hadoop.mr.EsOutputFormat',
        keyClass='org.apache.hadoop.io.NullWritable',
        valueClass='org.elasticsearch.hadoop.mr.LinkedMapWritable',
        conf={
            'es.input.json': 'false',
            'es.nodes': config['es']['host'],
            'es.port': str(config['es']['port']),
            'es.resource': '%s/%s' % (config['agg']['index'],
                                      config['agg']['_type']),
            'es.mapping.id': 'cik',
            'es.write.operation': 'upsert'
        }
    )
