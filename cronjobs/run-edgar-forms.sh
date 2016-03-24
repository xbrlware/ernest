#!/bin/bash

echo 'downloading forms'

path=/home/emmett/ernest

python $path/scrape-edgar-forms.py --back-fill --start-date='2004-01-01' --section=both --form-types=3,4
