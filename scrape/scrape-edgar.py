#!/usr/bin/env python

import argparse

from datetime import date
from modules.scrape_edgar_index import EDGAR_INDEX
from modules.scrape_edgar_forms import EDGAR_INDEX_FORMS
from enrich.generic_meta_enrich import GENERIC_META_ENRICH

parser = argparse.ArgumentParser(description='Scrape EDGAR indices')

parser.add_argument('--date', type=str, dest='date', action="store")
parser.add_argument('--from-scratch',
                    dest='from_scratch',
                    action="store_true")
parser.add_argument('--min-year',
                    type=int,
                    dest='min_year',
                    action="store",
                    default=2011)
parser.add_argument('--max-year',
                    type=int,
                    dest='max_year',
                    action="store",
                    default=int(date.today().year))
parser.add_argument('--most-recent',
                    dest='most_recent',
                    action="store_true")
parser.add_argument('--config-path',
                    type=str,
                    action='store',
                    default='../config.json')
parser.add_argument('--index', type=str, dest='index', action="store")
parser.add_argument('--expected', type=str, dest='expected', action="store")
parser.add_argument('--count-in', type=str, dest='count_in', action="store")
parser.add_argument('--count-out', type=str, dest='count_out', action="store")
parser.add_argument("--back-fill", action='store_true')
parser.add_argument("--start-date", type=str, action='store', required=True)
parser.add_argument("--end-date",
                    type=str,
                    action='store',
                    default=date.today().strftime('%Y-%m-%d'))
parser.add_argument("--form-types", type=str, action='store', required=True)
parser.add_argument("--section", type=str, action='store', default='both')

args = parser.parse_args()

ei = EDGAR_INDEX(args)
eif = EDGAR_INDEX_FORMS(args)
gme = GENERIC_META_ENRICH(args)

doc_count = ei.main()
gme.main(doc_count, 'edgar_index_cat')

doc_count = eif.main()
gme.main(doc_count, 'edgar_forms_cat')
