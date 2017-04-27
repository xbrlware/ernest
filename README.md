## Ernest

Code for scraping, processing and indexing data used by the Penny web-app found at:

    https://github.com/gophronesis/penny-sae

#### Dependencies

    Python (2.7) - core functionality
        Install external modules w/
            `pip install -r requirements.txt`
    
    R (3.x) - leveraged some prexisting libraries
        Install external packages w/
            `rscript packages.R`
    
    Elasticsearch (2.x) - for data storage
         
#### Project Info

The repo contains code for gathering, indexing and enriching a fairly large number of datatypes, including

    EDGAR filing indices, ownership forms, and XBRL data
    Trading suspensions and halts
    Press releases
    Paid stock promotions
    Online message boards

Core functionality is broken up as follows:
    
    ./scrape - Data collection
    ./enrich - Creating additional fields, linking data across sources
    ./aggregate - Aggregate datasets to form central index for `penny-sae`
    ./config.json - Set names of Elasticsearch indices
    
Other functionality:
    
    ./investor-forums - Specialized code for gathering and analyzing investor forums
    ./cronjobs - Daily script runners
    ./dev - New (or old) code that's not quite ready for production
    ./reference - Lookups, etc
    ./helpers - Miscellaneous functions
    
More documentation is included in the individual scripts.  A good place to start is `cronjobs/_run-all.sh`, which gets run each day to update the various datasets.

#### Issues
https://github.com/gophronesis/ernest/issues

#### Acknowledgements
This work was partially funded as part of the DARPA XDATA program.
