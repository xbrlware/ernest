# Run with:
#   rscript packages.R

packages <- c('R.utils',  "Rcpp", "rvest", "XML", "XBRL")
install.packages(packages, repos="http://cran.rstudio.com/")