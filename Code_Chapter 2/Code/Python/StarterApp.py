
# coding: utf-8

# # Starter App
# * The following program creates the RDD from a list 
# * The list contains a retail banking transaction records summary with Account Number, Balance Amount
# * The RDD is created from the parallelized collection
# * The RDD can be sliced and diced the way we want

# # Use Cases
# * Find all the records that came in
# * Filter out only the good records from the transaction records. The Account number should start with SB and the amount should be greater than 0
# * Find all the high value transaction records with the transaction amount greater than 1000
# * FInd all the transaction records where the Account numbers are bad
# * Find all the transaction records where the Amounts are less than or equal to zero
# * Find a combined list of all the bad transaction records
# * Find the sum of all the transaction amounts
# * Find the maximum of all the transaction mounts
# * Find the minimum of all the transaction amounts
# * Find all the good Account numbers

# In[1]:

from decimal import Decimal
# Creation of the list from where the RDD is going to be created
acTransList = ["SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30","SB10010,7000", "CR10001,7000", "SB10002,-10"]
# Create the RDD
acTransRDD = sc.parallelize(acTransList)
# Apply filter and create another RDD of good transaction records
goodTransRecords = acTransRDD.filter(lambda trans: Decimal(trans.split(",")[1]) > 0).filter(lambda trans: (trans.split(",")[0]).startswith('SB') == True)    
# Apply filter and create another RDD of high value transaction records
highValueTransRecords = goodTransRecords.filter(lambda trans: Decimal(trans.split(",")[1]) > 1000)
# The function that identifies the bad amounts
badAmountLambda = lambda trans: Decimal(trans.split(",")[1]) <= 0
# The function that identifies bad accounts
badAcNoLambda = lambda trans: (trans.split(",")[0]).startswith('SB') == False
# Apply filter and create another RDD of bad amount records
badAmountRecords = acTransRDD.filter(badAmountLambda)
# Apply filter and create another RDD of bad account records
badAccountRecords = acTransRDD.filter(badAcNoLambda)
# Do the union of two RDDs and create another RDD
badTransRecords  = badAmountRecords.union(badAccountRecords)


# In[2]:

# Collect the values from the RDDs to the driver program
acTransRDD.collect()


# In[3]:

# Collect the values from the RDDs to the driver program
goodTransRecords.collect()


# In[4]:

# Collect the values from the RDDs to the driver program
highValueTransRecords.collect()


# In[5]:

# Collect the values from the RDDs to the driver program
badAccountRecords.collect()


# In[6]:

# Collect the values from the RDDs to the driver program
badAmountRecords.collect()


# In[7]:

# Collect the values from the RDDs to the driver program
badTransRecords.collect()


# In[8]:

# The function that calculates the sum
sumAmount = goodTransRecords.map(lambda trans: Decimal(trans.split(",")[1])).reduce(lambda a,b : a+b)
# The function that calculates the maximum
maxAmount = goodTransRecords.map(lambda trans: Decimal(trans.split(",")[1])).reduce(lambda a,b : a if a > b else b)
# The function that calculates the minimum
minAmount = goodTransRecords.map(lambda trans: Decimal(trans.split(",")[1])).reduce(lambda a,b : a if a < b else b)
# Combine all the elements
combineAllElements = acTransRDD.flatMap(lambda trans: trans.split(","))
# Find the good account numbers
allGoodAccountNos = combineAllElements.filter(lambda trans: trans.startswith('SB') == True)


# In[9]:

sumAmount


# In[10]:

maxAmount


# In[11]:

minAmount


# In[12]:

# Collect the values from the RDDs to the driver program
combineAllElements.collect()


# In[13]:

# Collect the values from the RDDs to the driver program
allGoodAccountNos.distinct().collect()


# In[ ]:



