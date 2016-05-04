library(Rcpp)
library(rvest)
library(XML)
library(XBRL)
options(stringsAsFactors = TRUE)

newdir = '/home/ubuntu/xbrl/2016/01'
unzippedFiles<-list.files(newdir)
finalDir<-file.path(newdir,'parsed')
dir.create(finalDir, showWarnings = FALSE) 

for(u in unzippedFiles){
    tryCatch({
            for(m in list.files(file.path(newdir, u))){
                if(length(grep(pattern="[[:digit:]].xml", x=m))==1) { 
                    print(m) 
                    inst      <- file.path(newdir, u, m)
                    xbrl.vars <- xbrlDoAll(inst, verbose=TRUE)
                    # change colnames
                    fact <- as.data.frame(xbrl.vars["fact"])
                    colnames(fact) <- c(gsub('^fact.', '', colnames(fact)))
                    element <- as.data.frame(xbrl.vars["element"])
                    colnames(element) <- c(gsub('^element.', '', colnames(element)))
					label <- as.data.frame(xbrl.vars["label"])
					colnames(label) <- c(gsub('^label.', '', colnames(label)))
                    # joins tables to fact 
					join1 <- merge(x = fact, y = element, by = "elementId", all.x = TRUE)
					join2 <- merge(x = join1, y = label, by = "elementId", all.x = TRUE)
                    # join fact to calc
					calc           <- as.data.frame(xbrl.vars["calculation"])
					colnames(calc) <- c(gsub('^calculation.', '', colnames(calc)))
					calc$ID        <-seq.int(nrow(calc))
					calc$elementId <-calc$toElementId
					out_tbl        <- merge(x = calc, y = join2, by = 'elementId', all = TRUE) 
					out            <- out_tbl[order(out_tbl$ID), ]                    
					title<-gsub("-|.xml", "", u)  
                    write.table(out, file = file.path(finalDir,paste0(title,'.csv')), sep = "," , append = TRUE)    
                    
                }
            }
        }, 
        error = function(e) {}
        )
}
