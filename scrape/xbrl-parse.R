library(Rcpp)
library(rvest)
library(XML)
library(XBRL)
options(stringsAsFactors = TRUE)


args   <- commandArgs(trailingOnly = TRUE)

newdir <- file.path('/home/ubuntu/sec/', args[1], args[2], sep = '') 

zippedFiles  <-list.files(newdir)
finalDir     <-file.path('/home/ubuntu/sec/parsed_min__', args[1], '__', args[2], sep='')
print(finalDir)
dir.create(finalDir, showWarnings = FALSE) 



buildFrame <- function(name) {
        x                   <- name
        name                <- as.data.frame(xbrl.vars[name])
        colnames(name)      <- c(gsub(paste('^', x, '.', sep = ""), '', colnames(name)))
        return(name)
}


for(u in zippedFiles){
    unzip(file.path(newdir, u), list=FALSE, overwrite=TRUE, junkpaths=FALSE, exdir='/home/ubuntu/sec/unzipped',
             unzip = "internal", setTimes=FALSE)
    tryCatch({
            for(m in list.files('/home/ubuntu/sec/unzipped')){
                if(length(grep(pattern="[[:digit:]].xml", x=m))==1) { 
                    print(m) 
                    inst      <- file.path('/home/ubuntu/sec/unzipped', m)
                    xbrl.vars <- xbrlDoAll(inst, verbose=FALSE)
                    
                    # build frames
                    fact    <- buildFrame('fact')
                    context <- buildFrame('context')
                    # joins tables to fact 
                    join1   <- merge(x = fact, y = context, by = "contextId", all.x = TRUE)
                    # write out file          
                    title   <-gsub("-|.xml", "", m)  
                    print(title)
                    loc    <- file.path(finalDir,paste0(title,'.csv'))
                    print(loc) 
                    write.table(join1, file = loc, sep = "," , append = TRUE)    
                    
                }
            }
        }, 
        error = function(e) {}
        )
        unlink("/home/ubuntu/unzipped/*")
}
