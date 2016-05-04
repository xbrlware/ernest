library(Rcpp)
library(rvest)
library(XML)
library(XBRL)
options(stringsAsFactors = TRUE)


args   <- commandArgs(trailingOnly = TRUE)

newdir <- paste('/Users/culhane/xbrl/', args[1], '/', args[2], sep='') 


unzippedFiles<-list.files(newdir)
finalDir     <-file.path(newdir,'parsed')
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
                    calc    <- buildFrame('calculation')
                    element <- buildFrame('element')
                    label   <- buildFrame('label')

                    # joins tables to fact 
                    join1 <- merge(x = fact, y = element, by = "elementId", all.x = TRUE)
                    join2 <- merge(x = join1, y = label, by = "elementId", all.x = TRUE)
                    
                    # join fact to calc
                    calc$ID        <-seq.int(nrow(calc))
                    calc$elementId <-calc$toElementId
                    out_tbl        <- merge(x = calc, y = join2, by = 'elementId', all = TRUE) 
                    out            <- out_tbl[order(out_tbl$ID), ]                    
                    title          <-gsub("-|.xml", "", u)  
                    write.table(out, file = file.path(finalDir,paste0(title,'.csv')), sep = "," , append = TRUE)    
                    
                }
            }
        }, 
        error = function(e) {}
        )
}


