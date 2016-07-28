#!/bin/bash

# Script is used to compute delinwuency from the information in the ernest_aq_forms index
# 
# The period and filer status are used to compute a deadline for the filing and then 
# the submission date is used to evaluate delinwuency
# 
# Run daily to ensure data in ernest_aq_forms is current with available information 


echo "run-compute-delinquency"
python ../enrich/compute-delinquency.py
