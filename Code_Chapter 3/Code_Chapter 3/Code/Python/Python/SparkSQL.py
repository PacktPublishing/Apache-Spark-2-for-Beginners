
# coding: utf-8

# In[1]:

from pyspark.sql import Row
# Creation of the list from where the RDD is going to be created
acTransList = ["SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30","SB10010,7000", "CR10001,7000", "SB10002,-10"]
# Create the DataFrame
acTransDF = sc.parallelize(acTransList).map(lambda trans: trans.split(",")).map(lambda p: Row(accNo=p[0], tranAmount=float(p[1]))).toDF()
# Register temporary table in the DataFrame for using it in SQL
acTransDF.createOrReplaceTempView("trans")


# In[2]:

# Print the structure of the DataFrame
acTransDF.printSchema()


# In[3]:

# Show the first few records of the DataFrame
acTransDF.show()


# In[4]:

# Use SQL to create another DataFrame containing the good transaction records
goodTransRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE accNo like 'SB%' AND tranAmount > 0")
# Register temporary table in the DataFrame for using it in SQL
goodTransRecords.createOrReplaceTempView("goodtrans")
# Show the first few records of the DataFrame
goodTransRecords.show()


# In[5]:

# Use SQL to create another DataFrame containing the high value transaction records
highValueTransRecords = spark.sql("SELECT accNo, tranAmount FROM goodtrans WHERE tranAmount > 1000")
# Show the first few records of the DataFrame
highValueTransRecords.show()


# In[6]:

# Use SQL to create another DataFrame containing the bad account records
badAccountRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE accNo NOT like 'SB%'")
# Show the first few records of the DataFrame
badAccountRecords.show()


# In[7]:

# Use SQL to create another DataFrame containing the bad amount records
badAmountRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE tranAmount < 0")
# Show the first few records of the DataFrame
badAmountRecords.show()


# In[8]:

# Do the union of two DataFrames and create another DataFrame
badTransRecords = badAccountRecords.union(badAmountRecords)
# Show the first few records of the DataFrame
badTransRecords.show()


# In[9]:

# Calculate the sum
sumAmount = spark.sql("SELECT sum(tranAmount)as sum FROM goodtrans")
# Show the first few records of the DataFrame
sumAmount.show()


# In[10]:

# Calculate the maximum
maxAmount = spark.sql("SELECT max(tranAmount) as max FROM goodtrans")
# Show the first few records of the DataFrame
maxAmount.show()


# In[11]:

# Calculate the minimum
minAmount = spark.sql("SELECT min(tranAmount)as min FROM goodtrans")
# Show the first few records of the DataFrame
minAmount.show()


# In[12]:

# Use SQL to create another DataFrame containing the good account numbers
goodAccNos = spark.sql("SELECT DISTINCT accNo FROM trans WHERE accNo like 'SB%' ORDER BY accNo")
# Show the first few records of the DataFrame
goodAccNos.show()


# In[13]:

# Calculate the sum using mixing of DataFrame and RDD like operations
sumAmountByMixing = goodTransRecords.rdd.map(lambda trans: trans.tranAmount).reduce(lambda a,b : a+b)
sumAmountByMixing


# In[14]:

# Calculate the maximum using mixing of DataFrame and RDD like operations
maxAmountByMixing = goodTransRecords.rdd.map(lambda trans: trans.tranAmount).reduce(lambda a,b : a if a > b else b)
maxAmountByMixing


# In[15]:

# Calculate the minimum using mixing of DataFrame and RDD like operations
minAmountByMixing = goodTransRecords.rdd.map(lambda trans: trans.tranAmount).reduce(lambda a,b : a if a < b else b)
minAmountByMixing


# In[16]:

# Show the first few records of the DataFrame
acTransDF.show()


# In[17]:

# Print the structure of the DataFrame
acTransDF.printSchema()


# In[18]:

# Create the DataFrame using API for the good transaction records
goodTransRecords = acTransDF.filter("accNo like 'SB%'").filter("tranAmount > 0")
# Show the first few records of the DataFrame
goodTransRecords.show()


# In[19]:

# Create the DataFrame using API for the high value transaction records
highValueTransRecords = goodTransRecords.filter("tranAmount > 1000")
# Show the first few records of the DataFrame
highValueTransRecords.show()


# In[20]:

# Create the DataFrame using API for the bad account records
badAccountRecords = acTransDF.filter("accNo NOT like 'SB%'")
# Show the first few records of the DataFrame
badAccountRecords.show()


# In[21]:

# Create the DataFrame using API for the bad amount records
badAmountRecords = acTransDF.filter("tranAmount < 0")
# Show the first few records of the DataFrame
badAmountRecords.show()


# In[22]:

# Do the union of two DataFrames and create another DataFrame
badTransRecords = badAccountRecords.union(badAmountRecords)
# Show the first few records of the DataFrame
badTransRecords.show()


# In[23]:

# Calculate the sum
sumAmount = goodTransRecords.agg({"tranAmount": "sum"})
# Show the first few records of the DataFrame
sumAmount.show()


# In[24]:

# Calculate the maximum
maxAmount = goodTransRecords.agg({"tranAmount": "max"})
# Show the first few records of the DataFrame
maxAmount.show()


# In[25]:

# Calculate the minimum
minAmount = goodTransRecords.agg({"tranAmount": "min"})
# Show the first few records of the DataFrame
minAmount.show()


# In[26]:

# Create the DataFrame using API for the good account numbers
goodAccNos = acTransDF.filter("accNo like 'SB%'").select("accNo").distinct().orderBy("accNo")
# Show the first few records of the DataFrame
goodAccNos.show()


# In[27]:

# Persist the data of the DataFrame into a Parquet file
acTransDF.write.parquet("python.trans.parquet")


# In[28]:

# Read the data into a DataFrame from the Parquet file
acTransDFfromParquet = spark.read.parquet("python.trans.parquet")


# In[29]:

# Show the first few records of the DataFrame
acTransDFfromParquet.show()


# In[30]:

from pyspark.sql import Row


# In[31]:

# Creation of the list from where the RDD is going to be created
acTransList = ["SB10001,1000", "SB10002,1200", "SB10001,8000","SB10002,400", "SB10003,300", "SB10001,10000","SB10004,500","SB10005,56","SB10003,30","SB10002,7000", "SB10001,-100","SB10002,-10"]
# Create the DataFrame
acTransDF = sc.parallelize(acTransList).map(lambda trans: trans.split(",")).map(lambda p: Row(accNo=p[0], tranAmount=float(p[1]))).toDF()
# Register temporary table in the DataFrame for using it in SQL
acTransDF.createOrReplaceTempView("trans")
# Use SQL to create another DataFrame containing the account summary records
acSummary = spark.sql("SELECT accNo, sum(tranAmount) as transTotal FROM trans GROUP BY accNo")
# Show the first few records of the DataFrame
acSummary.show()    


# In[32]:

# Create the DataFrame using API for the account summary records
acSummaryViaDFAPI = acTransDF.groupBy("accNo").agg({"tranAmount": "sum"}).selectExpr("accNo", "`sum(tranAmount)` as transTotal")
# Show the first few records of the DataFrame
acSummaryViaDFAPI.show()


# In[33]:

# Creation of the list from where the RDD is going to be created
AcMaster = Row('accNo', 'firstName', 'lastName')
AcBal = Row('accNo', 'balanceAmount')
acMasterList = ["SB10001,Roger,Federer","SB10002,Pete,Sampras", "SB10003,Rafael,Nadal","SB10004,Boris,Becker", "SB10005,Ivan,Lendl"]
acBalList = ["SB10001,50000", "SB10002,12000","SB10003,3000", "SB10004,8500", "SB10005,5000"]
# Create the DataFrame
acMasterDF = sc.parallelize(acMasterList).map(lambda trans: trans.split(",")).map(lambda r: AcMaster(*r)).toDF()
acBalDF = sc.parallelize(acBalList).map(lambda trans: trans.split(",")).map(lambda r: AcBal(r[0], float(r[1]))).toDF()
# Persist the data of the DataFrame into a Parquet file
acMasterDF.write.parquet("python.master.parquet")
# Persist the data of the DataFrame into a JSON file
acBalDF.write.json("pythonMaster.json")
# Read the data into a DataFrame from the Parquet file
acMasterDFFromFile = spark.read.parquet("python.master.parquet")
# Register temporary table in the DataFrame for using it in SQL
acMasterDFFromFile.createOrReplaceTempView("master")
# Register temporary table in the DataFrame for using it in SQL
acBalDFFromFile = spark.read.json("pythonMaster.json")
# Register temporary table in the DataFrame for using it in SQL
acBalDFFromFile.createOrReplaceTempView("balance")
# Show the first few records of the DataFrame
acMasterDFFromFile.show()


# In[34]:

# Show the first few records of the DataFrame
acBalDFFromFile.show()


# In[35]:

# Use SQL to create another DataFrame containing the account detail records
acDetail = spark.sql("SELECT master.accNo, firstName, lastName, balanceAmount FROM master, balance WHERE master.accNo = balance.accNo ORDER BY balanceAmount DESC")
# Show the first few records of the DataFrame
acDetail.show()


# In[36]:

# Create the DataFrame using API for the account detail records
acDetailFromAPI = acMasterDFFromFile.join(acBalDFFromFile, acMasterDFFromFile.accNo == acBalDFFromFile.accNo).sort(acBalDFFromFile.balanceAmount, ascending=False).select(acMasterDFFromFile.accNo, acMasterDFFromFile.firstName, acMasterDFFromFile.lastName, acBalDFFromFile.balanceAmount)
# Show the first few records of the DataFrame
acDetailFromAPI.show()


# In[37]:

# Use SQL to create another DataFrame containing the top 3 account detail records
acDetailTop3 = spark.sql("SELECT master.accNo, firstName, lastName, balanceAmount FROM master, balance WHERE master.accNo = balance.accNo ORDER BY balanceAmount DESC").limit(3)
# Show the first few records of the DataFrame
acDetailTop3.show()


# In[ ]:



