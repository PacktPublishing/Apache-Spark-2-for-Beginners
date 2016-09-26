
# coding: utf-8

# # MapReduce App
# * The following program creates the RDD from a list 
# * The list contains a retail banking transaction records summary with Account Number, Balance Amount
# * The RDD is created from the parallelized collection
# * The RDD can be sliced and diced the way we want to demonstrate one MapReduce usecase

# # Use Cases
# * Find all the records that came in
# * Pair the accounts to have key/value pairs such as (AccNo, TranAmount)
# * Find an account level summary of all the transactions to get the account balance

# In[1]:

from decimal import Decimal
# Creation of the list from where the RDD is going to be created
acTransList = ["SB10001,1000", "SB10002,1200", "SB10001,8000", "SB10002,400", "SB10003,300", "SB10001,10000", "SB10004,500", "SB10005,56", "SB10003,30","SB10002,7000", "SB10001,-100", "SB10002,-10"]
# Create the RDD
acTransRDD = sc.parallelize(acTransList)
# Create the RDD containing key value pairs by doing mapping operation
acKeyVal = acTransRDD.map(lambda trans: (trans.split(",")[0],Decimal(trans.split(",")[1])))
# Create the RDD by reducing key value pairs by doing applying sum operation to the values
accSummary = acKeyVal.reduceByKey(lambda a,b : a+b).sortByKey()


# In[2]:

# Collect the values from the RDDs to the driver program
accSummary.collect()


# In[ ]:



