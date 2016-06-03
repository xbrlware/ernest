library(Rcpp)
library(rvest)
library(XML)
library(XBRL)
options(stringsAsFactors = TRUE)


args   <- commandArgs(trailingOnly = TRUE)

newdir <- paste('/Users/culhane/xbrl/', args[1], '/', args[2], sep='') 

unzippedFiles<-list.files(newdir)
finalDir     <-file.path(newdir,'parsed_min')
dir.create(finalDir, showWarnings = FALSE) 



buildFrame <- function(name) {
        x                   <- name
        name                <- as.data.frame(xbrl.vars[name])
        colnames(name)      <- c(gsub(paste('^', x, '.', sep = ""), '', colnames(name)))
        return(name)
}


for(u in unzippedFiles){
    tryCatch({
            for(m in list.files(file.path(newdir, u))){
                if(length(grep(pattern="[[:digit:]].xml", x=m))==1) { 
                    print(m) 
                    inst      <- file.path(newdir, u, m)
                    xbrl.vars <- xbrlDoAll(inst, verbose=TRUE)
                    
                    # build frames
                    fact    <- buildFrame('fact')
                    context <- buildFrame('context')
                    # joins tables to fact 
                    join1   <- merge(x = fact, y = context, by = "contextId", all.x = TRUE)
                    # write out file          
                    title   <-gsub("-|.xml", "", u)  
                    write.table(out, file = file.path(finalDir,paste0(title,'.csv')), sep = "," , append = TRUE)    
                    
                }
            }
        }, 
        error = function(e) {}
        )
}
