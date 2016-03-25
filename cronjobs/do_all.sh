#!/bin/bash

cd /home/emmett/ernest/cronjobs/ && ./run-edgar-index.sh
cd /home/ubuntu/edward/cronjobs/ && ./run-edgar-forms.sh
cd /home/ubuntu/edward/cronjobs/ && ./run-symbology.sh
cd /home/ubuntu/edward/cronjobs/ && ./run-ownership.sh

