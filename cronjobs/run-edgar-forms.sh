#!/bin/bash

echo 'run-edgar-forms.sh'

python ../scrape-edgar-forms.py --back-fill \
    --start-date='2004-01-01' \
    --section=both \
    --form-types=3,4 \
    --config-path $1
