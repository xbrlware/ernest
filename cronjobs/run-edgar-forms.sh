#!/bin/bash

echo 'run-edgar-forms.sh'

python ../scrape/scrape-edgar-forms.py --back-fill \
    --start-date='2016-05-29' \
    --section=both \
    --form-types=3,4 
