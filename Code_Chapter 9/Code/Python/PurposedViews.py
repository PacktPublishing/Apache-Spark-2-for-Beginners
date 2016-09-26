
# coding: utf-8

# In[31]:

from pyspark.sql import Row
#TODO: Change the following directory to point to your data directory
dataDir = "/Users/RajT/Documents/Writing/SparkForBeginners/To-PACKTPUB/Contents/B05289-09-DesigningSparkApplications/Code/Data/"


# In[32]:

#Load the user data into an RDD
userDataRDD = sc.textFile(dataDir + "user.txt").map(lambda line: line.split("|")).map(lambda p: Row(Id=int(p[0]), UserName=p[1], FirstName=p[2], LastName=p[3], EMail=p[4], AlternateEmail=p[5], Phone=p[6]))
#Convert the RDD into data frame
userDataDF = userDataRDD.toDF()
userDataDF.createOrReplaceTempView("user")
userDataDF.show()


# In[33]:

#Load the follower data into an RDD
followerDataRDD = sc.textFile(dataDir + "follower.txt").map(lambda line: line.split("|")).map(lambda p: Row(Follower=p[0], Followed=p[1]))
#Convert the RDD into data frame
followerDataDF = followerDataRDD.toDF()
followerDataDF.createOrReplaceTempView("follow")
followerDataDF.show()


# In[34]:

#Load the message data into an RDD
messageDataRDD = sc.textFile(dataDir + "message.txt").map(lambda line: line.split("|")).map(lambda p: Row(UserName=p[0], MessageId=int(p[1]), ShortMessage=p[2], Timestamp=int(p[3])))
#Convert the RDD into data frame
messageDataDF = messageDataRDD.toDF()
messageDataDF.createOrReplaceTempView("message")
messageDataDF.show()


# In[35]:

#Create the purposed view of the message to users
messagetoUsersRDD = messageDataRDD.filter(lambda message: "@" in message.ShortMessage).map(lambda message : (message, " ".join(filter(lambda s: s[0] == '@', message.ShortMessage.split(" "))))).map(lambda msgTuple: Row(FromUserName=msgTuple[0].UserName, ToUserName=msgTuple[1][1:], MessageId=msgTuple[0].MessageId, ShortMessage=msgTuple[0].ShortMessage, Timestamp=msgTuple[0].Timestamp))
#Convert the RDD into data frame
messagetoUsersDF = messagetoUsersRDD.toDF()
messagetoUsersDF.createOrReplaceTempView("messageToUsers")
messagetoUsersDF.show()


# In[36]:

#Create the purposed view of tagged messages 
taggedMessageRDD = messageDataRDD.filter(lambda message: "#" in message.ShortMessage).map(lambda message : (message, " ".join(filter(lambda s: s[0] == '#', message.ShortMessage.split(" "))))).map(lambda msgTuple: Row(HashTag=msgTuple[1], UserName=msgTuple[0].UserName, MessageId=msgTuple[0].MessageId, ShortMessage=msgTuple[0].ShortMessage, Timestamp=msgTuple[0].Timestamp))
#Convert the RDD into data frame
taggedMessageDF = taggedMessageRDD.toDF()
taggedMessageDF.createOrReplaceTempView("taggedMessages")
taggedMessageDF.show()


# In[37]:

#The following are the queries given in the use cases
#Find the messages that are grouped by a given hash tag
byHashTag = spark.sql("SELECT a.UserName, b.FirstName, b.LastName, a.MessageId, a.ShortMessage, a.Timestamp FROM taggedMessages a, user b WHERE a.UserName = b.UserName AND HashTag = '#Barcelona' ORDER BY a.Timestamp")
byHashTag.show()


# In[38]:

#Find the messages that are addressed to a given user
byToUser = spark.sql("SELECT FromUserName, ToUserName, MessageId, ShortMessage, Timestamp FROM messageToUsers WHERE ToUserName = 'wbryson' ORDER BY Timestamp")
byToUser.show()


# In[39]:

#Find the followers of a given user
followers = spark.sql("SELECT b.FirstName as FollowerFirstName, b.LastName as FollowerLastName, a.Followed FROM follow a, user b WHERE a.Follower = b.UserName AND a.Followed = 'wbryson'")
followers.show()


# In[40]:

#Find the followed users of a given user
followedUsers = spark.sql("SELECT b.FirstName as FollowedFirstName, b.LastName as FollowedLastName, a.Follower FROM follow a, user b WHERE a.Followed = b.UserName AND a.Follower = 'eharris'")
followedUsers.show()


# In[ ]:



