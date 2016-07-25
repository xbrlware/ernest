## Ernest

Code for scraping, processing and indexing data used by the Penny web-app found at:

    https://github.com/gophronesis/penny-sae

#### Getting started

The repo contains code for gathering, indexing and enriching a fairly large number of datatypes, including

    EDGAR filing indices, ownership forms, and XBRL data
    Trading suspensions and halts
    Press releases
    Paid stock promotions
    Online message boards

More documentation is included in the individual scripts.  A good place to start is `cronjobs/_run-all.sh`, which gets run each day to update the various datasets.

#### Questions and Comments
https://github.com/gophronesis/ernest/issues

#### Acknowledgements
This work was partially funded as part of the DARPA XDATA program.