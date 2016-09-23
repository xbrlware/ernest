#### STAT 405/705 Fall 2016 ASSIGNMENT 2
#### NAME: Emmett Culhane

#### If in a question I refer to a function that we have not seen in class, 
#### then use the help facility to find out about it.
#### Insert your answers under each question.

#### Download the .Rdata file called "HW2_2016Fall.Rdata" available on Canvas and read it in to R using R-studio.
#### This will bring all the objects you need for the homework straight into R-studio.


#### Q1. you will have read in a list called "gala.list". It contains the text based responses 
#### to a survey question that asked individuals what they liked "least" about a gala event.
#### The names in the list are respondent IDs (coerced to characters) and the associated element is the text response.


#a. Using the "lapply" and "unlist" commands find out which respondent had the longest response in terms of
#   the number of words.  What did this respondent say?
#   Provide your R code and answer

data    <-load("/Users/culhane/Desktop/HW2_2016Fall.Rdata") 

# ___ define function to get length for each response

opp     <- function(x){ 
    o <- strsplit(unlist(x[[1]]), " ")
    x <- length(o[[1]])
}

# ___ build frequency list

outList <- lapply(gala.list, function(x) opp(x))

# ___ define max index function

getMax <- function(){
    for (i in 1:length(outList)){
        if (outList[[i]] == max(unlist(outList))){
            return (gala.list[i])
        }
        else{}
    }
}

# ___ get element from gala.list

maxID <- getMax() 

# ___ the respondant with the longest response had the id number 

names(maxID)

## this individual complained about various aspects of the food: 
## forks, salad, dessert lines, dessert line location, ice cream & beef 
## they praised the nachos. The command bellow returns the full text.

maxID[[1]]


#b. Sort the elements in gala.list by the number of words, in decreasing order.  
#   Store your answer in a new list named gala.list.sorted
#   In addition to what you learned in class, you need the function "order".  
#   Check out what it does using ?order
#   Provide your R code and answer


n_words <- sapply(gala.list, function(x) {
    length(unlist(strsplit(x, ' ')))
})

sorted_x <- gala.list[order(-n_words)]



#### Q2. You will have imported a list called "mystery.list".
#### Inspect the list and describe briefly in words what is in it.

#a. Description:

#This list contains the first name, last name, and address (City, Street, Number) of three people.

mystery.list
length(mystery.list)
names(mystery.list)

#b. Convert mystery.list into a data frame named mystery.df with column names 
#   FIRSTNAME, LASTNAME, ADDRESS.NUM, ADDRESS.STREET, ADDRESS.CITY
#   Give this data frame rownames that are the initials 
#   (Amy Smith should have row name AS without space in between "A" and "S", etc.)
#   Do this in code, not manually.
#   
#   In addition to what you learned in class, you may need the following functions:
#     paste
#     substr
#   In particular, experiment to see how these functions work with vector arguments.


df           <- data.frame(matrix(unlist(mystery.list), nrow=length(mystery.list), byrow=T))
names(df)    <- c("FIRSTNAME", "LASTNAME", "ADDRESS.NUM", "ADDRESS.STREET", "ADDRESS.CITY")
rownames(df) <- paste(substr(as.character(df[,1]), 1, 1), substr(as.character(df[,2]), 1, 1), sep="")


#### Q3
# You will have read in a data frame called  "stock.df" which contains information on the price of Apple stock and Google stock on 
# all trading days in 2012

#a. How many rows and columns does this data frame have?

nrow(stock.df)
ncol(stock.df)

#b. Using the table command, find out if all 12 months in 2012 are represented in the data.  How many 
#   trading days was there in April?

length(table(stock.df$Month)) 

# yes all 12 months are represented

nrow(stock.df[stock.df$Month == "APR", ]) 

# there are 20 trading days in April

#c. On which days were Apple's stock price maximized?  How about Google's stock? 
#   Show your code that identifies the days. 

# __ google stock was maximized on 
maxGoogle <- stock.df[stock.df$Google.Price == max(stock.df$Google.Price), ]$Date

print(maxGoogle)

# __ apple stock was maximized on 
maxApple <- stock.df[stock.df$Apple.Price == max(stock.df$Apple.Price), ]$Date

print(maxApple)

#d. Create and add to the data frame two new columns that calculate the daily relative returns
#  of Apple stock and Google stock. Return is defined as (price today - price previous day)/(price previous day)
#  I suggest that you build these two vectors up outside of the data frame, and when complete, insert them back into the 
#  data frame. The first element in this vector will have to be an NA because there is no preceding day for it.
#  Provide your code that builds the return vector and the code that adds it back into the data frame.
#  There are many ways to create the vector of returns, but you may want to have a look at the "diff" function 
#  as an elegant way to perform the calculation.

# ___ define utility to compute relative returns

makeVec <- function(col, stock.df){
    V1 <- stock.df[, col]
    V2 <- vector()
        for (i in 1:length(V1)){ 
            if (i == 1) { 
                V2[i] = NA
            }
            else { 
                V2[i] = (V1[i] - V1[i-1]) / V1[i -1]
            }
        }
    return (V2)
} 

# ___ build vectors with function

rVecG <- makeVec("Google.Price", stock.df)

rVecA <- makeVec("Apple.Price", stock.df)


# ___ append vectors to dataframe

stock.df$Google.RR <- rVecG

stock.df$Apple.RR <- rVecA


#e. Calculate average daily returns for Apple and Google over this time period. To deal with the NA, use the "na.rm" 
# argument to the "mean" function.

Google.ADR <- mean(stock.df$Google.RR, na.rm=TRUE)

Apple.ADR <- mean(stock.df$Apple.RR, na.rm=TRUE)


#f. On how many days in 2012 did the daily relative returns exceed 5% for each stock?

nrow(stock.df[stock.df$Google.RR > 0.05, ]) 

nrow(stock.df[stock.df$Apple.RR > 0.05, ]) 


#g. The "by" command is useful for summarizing one variable over the levels of a categorical/factor variable. 
#   Read about the "by" command using R's help function.

#   Using the "by" command find which month had the highest average daily return for each stock.
#   Because the mean for January will be NA due to the first day being NA, you need to pass
#   the na.rm argument into the mean function when "by" applies it.
#   In the function syntax:
#   by(data, INDICES, FUN, ..., simplify = TRUE)
#   the three dots "..." is a special slot where you can pass in arguments to the function FUN 
#   This will allow you to find the mean daily return for January too. 
#
#   Please show your code, give the best month, and the average return for that month.

aOut <- by(stock.df$Apple.RR, stock.df$Month, mean, na.rm=TRUE, simplify=TRUE)

gOut <- by(stock.df$Google.RR, stock.df$Month, mean, na.rm=TRUE, simplify=TRUE)



#### Q4 Regression

#a. Calculate the correlation between Apple stock price and Google stock price.

cor(stock.df$Apple.Price, stock.df$Google.Price)


#b. Run a regression of Apple returns against the market returns and Google returns, separately. 
# Which variable is a better predictor between market returns and google returns? please justify your answer. 


rM <-lm(stock.df$Apple.RR~stock.df$Market.Return)

rG <-lm(stock.df$Apple.RR~stock.df$Google.RR)

# __ the market is a better predictor of Apple stock than google, using the summary command you can 
# __ see that the coeficient estimated in the Market regression is pretty close to 1, where it 
# __ is near .5 for the Google regression. The significance levels are basically the same for both.



#c. Start with the model that predicts Apple daily returns using Google daily returns in part (b), 
#   this may or may not have been the best model. 
#   Now add the categorical variable, Month, to the regression. 
#   Fit a model where each month gets a separate regression line, 
#   with a different intercept, but sharing the same slope.
#   Is Month, in the presence of Google daily returns, a statistically significant 
#   predictor of apple returns?
#   How much improvement in R-square do you get by adding Month? 
#   Show the code you used to generate the relevant output as well as your conclusion.

rX <-lm(stock.df$Apple.RR~stock.df$Google.RR + stock.df$Month)

fitted <- predict(rX, stock.df)
plot(fitted ~ stock.df$Google.RR)

# __ month does not seem to be a significant variable in the presence of Google.RR
# __ we get very little change in the adjusted R squared between the two specifications 
# __ without month the adj r squared is .1471, and with month it is .1438




