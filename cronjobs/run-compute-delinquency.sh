#!/bin/bash

# Script is used to compute delinquency from the information in the ernest_aq_forms index
# 
# The period and filer status are used to compute a deadline for the filing and then 
# the submission date is used to evaluate delinwuency
# 
# Run daily to ensure data in ernest_aq_forms is current with available information 
d=$(date +'%Y%m%d_%H%M%S')

python ../enrich/compute-delinquency.py  \
        --log-file="/home/ubuntu/ernest/cronjobs/logs/log_$d"
