#!/bin/bash

echo 'downloading forms'

path=/home/ubuntu/ernest

python $path/scrape-edgar-forms.py --back-fill --start-date='2004-01-01' --section=both --form-types=3,4 --config-path='/home/ubuntu/ernest/config.json'