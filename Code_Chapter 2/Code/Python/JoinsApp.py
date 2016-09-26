
# coding: utf-8

# # Joins App
# * The following program creates the RDD from lists 
# * The first list contains a retail banking master records summary with Account Number, First Name, Last Name
# * The second list contains the retail banking transactions with Account Number, Balance Amount
# * The RDD is created from the parallelized collection from the above two lists
# * The two RDDs are joined on the account number

# # Use Case
# * Two different data sets are available
# * One contains the master account details such as account number, first name and last name
# * The other contains the account balance details such as account number and balance
# * Join the two data sets and create one data set containing account number, first name, last name and balance
# * From the list containing Account Number, Name and Account Balance get the one that has the highest Account Balance
# * From the list containing Account Number, Name and Account Balance get the top three having the highest Account Balance
# * Count the number of balance transaction records at an account level
# * Count the total number of balance transaction records
# * Print the Name and  Account Balance of all the accounts.
# * Calculate the total of the Account Balance

# In[1]:

# Creation of the list from where the RDD is going to be created
acMasterList = ["SB10001,Roger,Federer", "SB10002,Pete,Sampras", "SB10003,Rafel,Nadal", "SB10004,Boris,Becker", "SB10005,Ivan,Lendl"]
# Creation of the list from where the RDD is going to be created
acBalList = ["SB10001,50000", "SB10002,12000", "SB10003,3000", "SB10004,8500", "SB10005,5000"]
# Create the RDD
acMasterRDD = sc.parallelize(acMasterList)
# Create the RDD
acBalRDD = sc.parallelize(acBalList)


# In[2]:

# Collect the values to the driver program
acMasterRDD.collect()


# In[3]:

# Collect the values to the driver program
acBalRDD.collect()


# In[4]:

# Create account master tuples
acMasterTuples = acMasterRDD.map(lambda master: master.split(",")).map(lambda masterList: (masterList[0], masterList[1] + " " + masterList[2]))
# Create balance tuples
acBalTuples = acBalRDD.map(lambda trans: trans.split(",")).map(lambda transList: (transList[0], transList[1]))


# In[5]:

# Collect the values to the driver program
acMasterTuples.collect()


# In[6]:

# Collect the values to the driver program
acBalTuples.collect()


# In[7]:

# Join the tuples
acJoinTuples = acMasterTuples.join(acBalTuples).sortByKey().map(lambda tran: (tran[0], tran[1][0],tran[1][1]))


# In[8]:

# Collect the values to the driver program
acJoinTuples.collect()


# In[9]:

# Find the account name and balance
acNameAndBalance = acJoinTuples.map(lambda tran: (tran[1],tran[2]))


# In[10]:

# Collect the values to the driver program
acNameAndBalance.collect()


# In[11]:

from decimal import Decimal
# Find the account tuples sorted by amount
acTuplesByAmount = acBalTuples.map(lambda tran: (Decimal(tran[1]), tran[0])).sortByKey(False)


# In[12]:

# Collect the values to the driver program
acTuplesByAmount.collect()


# In[13]:

# Get the top element
acTuplesByAmount.first()


# In[14]:

# Get the top 3 elements
acTuplesByAmount.take(3)


# In[15]:

# Count by the key
acBalTuples.countByKey()


# In[16]:

# Count all the records
acBalTuples.count()


# In[17]:

# Print the contents of the account name and balance RDD
acNameAndBalance.foreach(print)


# In[18]:

# Find the balance total using accumulator
balanceTotal = sc.accumulator(0.0)
balanceTotal.value


# In[19]:

# Do the summation
acBalTuples.foreach(lambda bals: balanceTotal.add(float(bals[1])))


# In[20]:

# Print the results
balanceTotal.value


# In[ ]:



