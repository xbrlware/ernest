require(R.utils, warn.conflicts = FALSE)
library(Rcpp, warn.conflicts = FALSE)
library(rvest, warn.conflicts = FALSE)
library(XML, warn.conflicts = FALSE)
library(XBRL, warn.conflicts = FALSE)

options(stringsAsFactors = TRUE)

args <- commandArgs(trailingOnly = TRUE)

finalDir <-file.path(paste('/home/ubuntu/sec/parsed_min__', args[1], '__0', args[2], sep=''))
unzippedDir <-file.path(paste('/home/ubuntu/sec/unzipped__', args[1], '__0', args[2], sep=''))
unzippedFiles <-list.files(unzippedDir)

print(finalDir)
print(unzippedDir)
dir.create(finalDir) 

buildFrame <- function(name, xbrl.vars) {
        x                   <- name
        name                <- as.data.frame(xbrl.vars[name])
        colnames(name)      <- c(gsub(paste('^', x, '.', sep = ""), '', colnames(name)))
        return(name)
}

parseDoc <- function(finalDir, unzippedDir) {
    tryCatch({
            for(m in list.files(unzippedDir)){
                if(length(grep(pattern="[[:digit:]].xml", x=m))==1) { 
                    print(m) 
                    inst      <- file.path(unzippedDir, m)
                    xbrl.vars <- xbrlDoAll(inst, verbose=FALSE)
                    
                    # build frames
                    fact    <- buildFrame('fact', xbrl.vars)
                    context <- buildFrame('context', xbrl.vars)
                    # joins tables to fact 
                    join1   <- merge(x = fact, y = context, by = "contextId", all.x = TRUE)
                    # write out file          
                    title   <-gsub("-|.xml", "", m)  
                    print(title)
                    loc    <- file.path(finalDir,paste0(title,'.csv'))
                    print(loc) 
                    write.table(join1, file = loc, sep = "," , append = TRUE)    
                    unlink(paste(unzippedDir, '/*', sep = ''))
                }
            }
        }, 
        error = function(e) {unlink(paste(unzippedDir, '/*', sep = ''))}
        )
}

tryCatch(
    expr = {
        evalWithTimeout(
            {parseDoc(finalDir, unzippedDir)}, timeout = 300)},
             TimeoutException = function(ex) cat("Timeout. Skipping."))

unlink(paste(unzippedDir, '/*', sep = ''))
