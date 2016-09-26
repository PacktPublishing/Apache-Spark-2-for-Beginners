# SPARK_HOME settings and SparkR library path settings
# When you are running this script from your setup, please make sure that it is pointing to your SPARK_HOME directory
SPARK_HOME_DIR <- "/Users/RajT/source-code/spark-source/spark-2.0"
DATA_DIR <- "/Users/RajT/Documents/Writing/SparkForBeginners/To-PACKTPUB/Contents/B05289-04-SparkProgrammingWithR/CodeAndData/R/"
Sys.setenv(SPARK_HOME=SPARK_HOME_DIR)
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
spark <- sparkR.session(master="local[*]")

#Fundamentals of R
# Variable
x <- 5
x
# Numerical vector
aNumericVector <- c(10,10.5,31.2,100)
aNumericVector
# Character vector
aCharVector <- c("apple", "orange", "mango")
aCharVector
# Boolean Vector
aBooleanVector <- c(TRUE, FALSE, TRUE, FALSE, FALSE)
aBooleanVector
# List containing multiple vectors
aList <- list(aNumericVector, aCharVector)
aList
# Matrix creation
aMatrix <- matrix(c(100, 210, 76, 65, 34, 45),nrow=3,ncol=2,byrow = TRUE)
aMatrix
bMatrix <- matrix(c(100, 210, 76, 65, 34, 45),nrow=3,ncol=2,byrow = FALSE)
bMatrix
# Create R DataFrame using vectors
ageVector <- c(21, 35, 52) 
nameVector <- c("Thomas", "Mathew", "John")
marriedVector <- c(FALSE, TRUE, TRUE)
aDataFrame <- data.frame(ageVector, nameVector, marriedVector)
colnames(aDataFrame) <- c("Age","Name", "Married")
# DIsplay the contents from the DataFrame
head(aDataFrame)
tail(aDataFrame)
nrow(aDataFrame)
ncol(aDataFrame)
aDataFrame[1]
aDataFrame[2]
aDataFrame[c("Age", "Name")]
aDataFrame[[2]]
aDataFrame[2,]
aDataFrame[c(1,2),]

#Spark DataFrame programming with R - Basics
# Read data from a JSON file to create DataFrame
acTransDF <- read.json(paste(DATA_DIR, "TransList1.json", sep = ""))
# Print the structure of the DataFrame
print(acTransDF)
# Show sample records from the DataFrame
showDF(acTransDF)
# Register temporary view definition in the DataFrame for SQL queries
createOrReplaceTempView(acTransDF, "trans")
# DataFrame containing good transaction records using SQL
goodTransRecords <- sql("SELECT AccNo, TranAmount FROM trans WHERE AccNo like 'SB%' AND TranAmount > 0")
# Register temporary table definition in the DataFrame for SQL queries
createOrReplaceTempView(goodTransRecords, "goodtrans")
# Show sample records from the DataFrame
showDF(goodTransRecords)
# DataFrame containing high value transaction records using SQL
highValueTransRecords <- sql("SELECT AccNo, TranAmount FROM goodtrans WHERE TranAmount > 1000")
# Show sample records from the DataFrame
showDF(highValueTransRecords)
# DataFrame containing bad account records using SQL
badAccountRecords <- sql("SELECT AccNo, TranAmount FROM trans WHERE AccNo NOT like 'SB%'")
# Show sample records from the DataFrame
showDF(badAccountRecords)
# DataFrame containing bad amount records using SQL
badAmountRecords <- sql("SELECT AccNo, TranAmount FROM trans WHERE TranAmount < 0")
# Show sample records from the DataFrame
showDF(badAmountRecords)
# Create a DataFrame by taking the union of two DataFrames
badTransRecords <- union(badAccountRecords, badAmountRecords)
# Show sample records from the DataFrame
showDF(badTransRecords)
# DataFrame containing sum amount using SQL
sumAmount <- sql("SELECT sum(TranAmount) as sum FROM goodtrans")
# Show sample records from the DataFrame
showDF(sumAmount)
# DataFrame containing maximum amount using SQL
maxAmount <- sql("SELECT max(TranAmount) as max FROM goodtrans")
# Show sample records from the DataFrame
showDF(maxAmount)
# DataFrame containing minimum amount using SQL
minAmount <- sql("SELECT min(TranAmount)as min FROM goodtrans")
# Show sample records from the DataFrame
showDF(minAmount)
# DataFrame containing good account number records using SQL
goodAccNos <- sql("SELECT DISTINCT AccNo FROM trans WHERE AccNo like 'SB%' ORDER BY AccNo")
# Show sample records from the DataFrame
showDF(goodAccNos)
# DataFrame containing good transaction records using API
goodTransRecordsFromAPI <- filter(acTransDF, "AccNo like 'SB%' AND TranAmount > 0")
# Show sample records from the DataFrame
showDF(goodTransRecordsFromAPI)
# DataFrame containing high value transaction records using API
highValueTransRecordsFromAPI = filter(goodTransRecordsFromAPI, "TranAmount > 1000")
# Show sample records from the DataFrame
showDF(highValueTransRecordsFromAPI)
# DataFrame containing bad account records using API
badAccountRecordsFromAPI <- filter(acTransDF, "AccNo NOT like 'SB%'")
# Show sample records from the DataFrame
showDF(badAccountRecordsFromAPI)
# DataFrame containing bad amount records using API
badAmountRecordsFromAPI <- filter(acTransDF, "TranAmount < 0")
# Show sample records from the DataFrame
showDF(badAmountRecordsFromAPI)
# Create a DataFrame by taking the union of two DataFrames
badTransRecordsFromAPI <- union(badAccountRecordsFromAPI, badAmountRecordsFromAPI)
# Show sample records from the DataFrame
showDF(badTransRecordsFromAPI)
# DataFrame containing sum amount using API
sumAmountFromAPI <- agg(goodTransRecordsFromAPI, sumAmount = sum(goodTransRecordsFromAPI$TranAmount))
# Show sample records from the DataFrame
showDF(sumAmountFromAPI)
# DataFrame containing maximum amount using API
maxAmountFromAPI <- agg(goodTransRecordsFromAPI, maxAmount = max(goodTransRecordsFromAPI$TranAmount))
# Show sample records from the DataFrame
showDF(maxAmountFromAPI)
# DataFrame containing minimum amount using API
minAmountFromAPI <- agg(goodTransRecordsFromAPI, minAmount = min(goodTransRecordsFromAPI$TranAmount)) 
# Show sample records from the DataFrame
showDF(minAmountFromAPI)
# DataFrame containing good account number records using API
filteredTransRecordsFromAPI <- filter(goodTransRecordsFromAPI, "AccNo like 'SB%'")
accNosFromAPI <- select(filteredTransRecordsFromAPI, "AccNo")
distinctAccNoFromAPI <- distinct(accNosFromAPI)
sortedAccNoFromAPI <- arrange(distinctAccNoFromAPI, "AccNo")
# Show sample records from the DataFrame
showDF(sortedAccNoFromAPI)
write.parquet(acTransDF, "r.trans.parquet")
acTransDFFromFile <- read.parquet("r.trans.parquet")
# Show sample records from the DataFrame
showDF(acTransDFFromFile)

#Aggregations
# Read data from a JSON file to create DataFrame
acTransDFForAgg <- read.json(paste(DATA_DIR, "TransList2.json", sep = ""))
# Register temporary view definition in the DataFrame for SQL queries
createOrReplaceTempView(acTransDFForAgg, "transnew")
# Show sample records from the DataFrame
showDF(acTransDFForAgg)
# DataFrame containing account summary records using SQL
acSummary <- sql("SELECT AccNo, sum(TranAmount) as TransTotal FROM transnew GROUP BY AccNo")
# Show sample records from the DataFrame
showDF(acSummary)
# DataFrame containing account summary records using API
acSummaryFromAPI <- agg(groupBy(acTransDFForAgg, "AccNo"), TranAmount="sum")
# Show sample records from the DataFrame
showDF(acSummaryFromAPI)

# Multi-data source joins
acMasterDF <- read.json(paste(DATA_DIR, "MasterList.json", sep = ""))
# Show sample records from the DataFrame
showDF(acMasterDF)
# Register temporary view definition in the DataFrame for SQL queries
createOrReplaceTempView(acMasterDF, "master")
acBalDF <- read.json(paste(DATA_DIR, "BalList.json", sep = ""))
# Show sample records from the DataFrame
showDF(acBalDF)
# Register temporary table definition in the DataFrame for SQL queries
createOrReplaceTempView(acBalDF, "balance")
# DataFrame containing account detail records using SQL by joining multiple DataFrame contents
acDetail <- sql("SELECT master.AccNo, FirstName, LastName, BalAmount FROM master, balance WHERE master.AccNo = balance.AccNo ORDER BY BalAmount DESC")
# Show sample records from the DataFrame
showDF(acDetail)
# Persist data in the DataFrame into Parquet file
write.parquet(acDetail, "r.acdetails.parquet")
# Read data into a DataFrame by reading the contents from a Parquet file
acDetailFromFile <- read.parquet("r.acdetails.parquet")
# Show sample records from the DataFrame
showDF(acDetailFromFile)
# Change the column names
acBalDFWithDiffColName <- selectExpr(acBalDF, "AccNo as AccNoBal", "BalAmount")
# Show sample records from the DataFrame
showDF(acBalDFWithDiffColName)
# DataFrame containing account detail records using API by joining multiple DataFrame contents
acDetailFromAPI <- join(acMasterDF, acBalDFWithDiffColName, acMasterDF$AccNo == acBalDFWithDiffColName$AccNoBal)
# Show sample records from the DataFrame
showDF(acDetailFromAPI)
# DataFrame containing account detail records using SQL by selecting specific fields
acDetailFromAPIRequiredFields <- select(acDetailFromAPI, "AccNo", "FirstName", "LastName", "BalAmount")
# Show sample records from the DataFrame
showDF(acDetailFromAPIRequiredFields)

