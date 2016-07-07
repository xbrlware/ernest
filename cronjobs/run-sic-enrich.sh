#!/bin/bash

echo 'run-sic-enrich.sh'

echo 'adding sic info to symbology documents...'
python ../enrich/enrich-add-sic-descs.py --index='symbology'

echo 'adding sic info to ownership documents...'
python ../enrich/enrich-add-sic-descs.py --index='ownership'