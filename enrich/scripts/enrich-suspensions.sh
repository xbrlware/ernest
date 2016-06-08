#!/bin/bash

echo '-- adding ciks to suspension'
python enrich-name2cik.py --index suspension --field-name company
