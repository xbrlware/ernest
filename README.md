### Elasticsearch Indices

    edgar_index_cat
      index of document meta-data for all filings hosted on the sec-ftp directory
      
    ernest_forms_cat: 
      index of full form 3 and form 4 ownership documents 

    ernest_symbology_cat:
      aggregation of form 3 and 4 company information with min and max dates 
      
    ernest_ownership_cat: 
      aggregation of form 3 and 4 ownership information with min and max dates
      
    ernest_delinquency: 
      index of 10-K and 10-Q metadata with filer status (from xbrl submissions files), filing 
      deadlines (using sec statutes), and a flag for delinquent submissions
      
    crowdsar_cat: 
      index of message board posts scraped from investors hub forum, with company information
      matched from the symbology index, and a three way classification score for post 
      sentiment [positive; negative; nuetral]; in the process of being rebuilt
      
    ernest_pv_cat: 
      index of daily equity capital markets information for each ticker in the crowdsar index
      pulled through the yahoo finance API
       
    ernest_otc_raw_cat: 
      index of daily over the counter equity transactions scraped from the finra OTCBB 
       directory [Over The Counter Bulletin Board]
       
    ernest_otce_halts_cat: 
      index of current and historical trading halts for otce companies; some of these will 
      overlap with the sec directory for trading halts and others will not
      
    ernest_otc_delinquency_cat: 
      index of non-compliant otc companies published each day on the finra website
      
    ernest_otc_directory_cat: 
      index of all otce companies published on the finra website
      
    xbrl_submissions_cat: 
      index of xbrl submissions files released each quarter by the sec through the annual 
      quarterly financial statements [AQFS] directory
      
    ernest_xbrl_aqfs: 
      index of joined aqfs submission, presentaton, tag & number files released quarterly 
      by the sec; the join produces a contextualized set of xbrl tags for the balance sheet
      cash flow and income statment sections of the earnings report
      
    ernest_omx_cat: 
      index of historical omx articles; currently running for old documents
      
    ernest_suspensions_cat: 
      index of sec trading halts 


# Docs

# _____________________________________________ ##### SCRAPE #####:

# ======================== scrape-edgar-index.py 

# __ args: 
--from-scratch [build whole index from scratch]
--min-year     [minimum year for from scratch argument]
--max-year     [maximum year for from scratch argument]
--most-recent  [download only most recent documents]
--config-path  [path to ernest config file]

# __ shell: 
run-edgar-index.sh

# __ write index: 
edgar_index_cat

# ======================== scrape-edgar-forms.py

# __ args: 
--back-fill    [get all docs from edgar_index that havent been grabbed]
--start-date   [start date for ingest]
--end-date     [end date for ingest]
--form-types   [types of forms to ingest]
--section      [section -- header, body or both]
--config-path  [/path/to/config/file]

# __ shell: 
run-edgar-forms.sh

# __ write index:
ernest_forms_cat

# ======================== scrape-sec-suspensions.py

# __ args: 
--config-path  [/path/to/config/file]
--start-date   [start date for scrape; default 2004-01-01]
--end-year     [end year for scrape range; default current year]
--stdout       [option envoked if json output should be written to file instead of indexed to elasticsearch]
--most-recent  [option to only download suspensions for the current year]
 
# __ shell: 
run-suspensions-scrape.sh

# __ write index: 
ernest_suspensions_cat

# ======================== scrape-otc.py

# __ args: 
--from-scratch   [option to rebuild entire index from scratch]
--config-path    [/path/to/config/file]
--most-recent    [only build for most recent year]

# __ shell: 
run-otc-scrape.sh

# __ write index: 
ernest_otc_raw_cat

# ======================== scrape-sic-codes.py

# __ args: 

# __ shell: 

# __ write index: 

# ======================== scrape-xbrl-submissions.py

# __ args: 
--from-scratch  [build index from scratch]
--most-recent   [only download / ingest most recent aqfs release]
--config-path   [/path/to/config/file]

# __ shell: 
run-xbrl-submission-scrape.sh

# __ write index: 
xbrl_submissions_cat

# ======================== build-delinquency.py

# __ args: 
--from-scratch  [build index from scratch]
--update        [only ingest new forms]
--config-path   [/path/to/config/file]
--status        [add filer status to forms]
--period        [add deadline to forms]

# __ shell: 
run-build-delinquency.sh

# __ write index: 
ernest_delinquency_cat
edgar_index_cat


# ======================== scrape-finra-directories.py

# __ args: 
--config-path  [/path/to/config/file]
--directory    [either halts; otc; delinquency]

# __ shell: 
run-finra-scrapes.sh

# __ write index: 
ernest_otce_halts_cat
ernest_otc_directory_cat
ernest_otc_delinquency_cat

# ======================== omx-scraper-html.py [not integrated]
# ======================== omx-scraper-rss.py  [not integrated]




# _____________________________________________ ##### ENRICH #####:

# ======================== compute-symbology.py

# __ args: 
--from-scratch  [build index from scratch]
--last-week     [only get most recent docs]
--config-path   [/path/to/config/file]
--testing       [flag to set for debugging]

# __ shell: 
run-symbology.sh

# __ write index: 
ernest_symbology_cat

# ======================== compute-ownership-graph.py

# __ args: 
--from-scratch  [build index from scratch]
--last-week     [only get most recent docs]
--config-path   [/path/to/config/file]

# __ shell: 
run-ownership.sh

# __ write index: 
ernest_ownership_cat

# ======================== compute-delinquency.py

# __ args: 
--config-path  [/path/to/config/file]

# __ shell: 
run-compute-delinquency.sh

# __ write index: 
ernest_delinquency_cat

# ======================== enrich-sic.py

# __ args: 
--config-path [/path/to/config/file]
--lookup-path [/path/to/sic/ref]
--index       [index to enrich: ownership; symbology]

# __ shell: 
run-enrich-sic.py

# __ write index: 
ernest_symbology_cat
ernest_ownership_cat

# ======================== enrich-otc.py

# __ args: 
--config-path [path/to/config/file]

# __ shell: 
run-enrich-otc.py

# __ write index: 
ernest_otc_raw_cat




# _______________________________________________ INVESTOR-FORUMS:


# ______________________________________ ##### Scrapers #####:

# ======================== 0_set_mapping.py

# __ args: 
--config-path  [/path/to/config/file]

# __ shell: 

# __ write index: 
crowdsar_cat

# ======================== 1_es_psm_scrape.py

# __ args: 
--config-path  [/path/to/config/file]
--lookup-path  [/path/to/lookup/file]

# __ shell: 
run-scrape.sh

# __ write index: 
crowdsar_cat



# ======================== build_pv.py

# __ args: 
--config-path  [/path/to/config/file]
--start-date   [start date for pv ingest]
--end-date     [end date for pv ingest]
--grab-new     [flag set to only download most recent docs]

# __ shell: 
run-daily.sh

# __ write index: 
ernest_pv_cat

# ======================== fill_gaps/ih_scrape_gaps.py

# __ args: 

# __ shell: 

# __ write index: 

# ======================== fill_gaps/load_es_posts_data.py

# __ args: 

# __ shell: 

# __ write index: 


# ______________________________________ ##### Augment #####: 

# ======================== add_cik_and_ticker.py

# __ args: 
--config-path  [/path/to/config/file]

# __ shell: 
run-daily.sh

# __ write index: 
crowdsar_cat

# ======================== tri_class_apply_model.py

# __ args: 
--config-path  [/path/to/config/file]
--model-path   [/path/to/model]
--back-fill    [flag set to only download apply model to new docs]
--start-date   [start date for model application]
--end-date     [end date for model application]


# __ shell: 
run-enrich-sic.py

# __ write index: 
crowdsar_cat
