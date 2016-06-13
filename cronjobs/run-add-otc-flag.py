#!/bin/bash

echo "running enrich-add-otc-flag.py"

python ../enrich/enrich-add-otc-flag.py --index ownership --field-name issuerTradingSymbol
python ../enrich/enrich-add-otc-flag.py --index symbology --field-name ticker