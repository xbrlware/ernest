#!/bin/bash

echo "run-build-delinquency"

echo "-- getting filer status --"
python ../scrape/build-delinquency.py --update --status

echo "-- getting filing deadlines --"
python ../scrape/build-delinquency.py --update --period