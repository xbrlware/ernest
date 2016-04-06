#!/bin/bash

cd /home/ubuntu/ernest/cronjobs/ && bash run-edgar-index.sh
cd /home/ubuntu/ernest/cronjobs/ && bash run-edgar-forms.sh
cd /home/ubuntu/ernest/cronjobs/ && bash run-symbology.sh
cd /home/ubuntu/ernest/cronjobs/ && bash run-ownership.sh

