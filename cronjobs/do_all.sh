#!/bin/bash

cd /home/ubuntu/ernest/cronjobs/ && ./run-edgar-index.sh
cd /home/ubuntu/ernest/cronjobs/ && ./run-edgar-forms.sh
cd /home/ubuntu/ernest/cronjobs/ && ./run-symbology.sh
cd /home/ubuntu/ernest/cronjobs/ && ./run-ownership.sh

