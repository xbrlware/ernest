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