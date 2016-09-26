/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.PurposedViews
**/


package com.packtpub.sfb

import org.apache.spark.sql.{Row, SparkSession}

object PurposedViews
{
	// Create the Spark Session and the spark context			
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	val sc = spark.sparkContext
	//TODO: Change the following directory to point to your data directory
	val dataDir = "/Users/RajT/Documents/Writing/SparkForBeginners/To-PACKTPUB/Contents/B05289-09-DesigningSparkApplications/Code/Data/"
	import spark.implicits._
	//Define the case classes in Scala for the entities
	case class User(Id: Long, UserName: String, FirstName: String, LastName: String, EMail: String, AlternateEmail: String, Phone: String)
	case class Follow(Follower: String, Followed: String)
	case class Message(UserName: String, MessageId: Long, ShortMessage: String, Timestamp: Long)
	case class MessageToUsers(FromUserName: String, ToUserName: String, MessageId: Long, ShortMessage: String, Timestamp: Long)
	case class TaggedMessage(HashTag: String, UserName: String, MessageId: Long, ShortMessage: String, Timestamp: Long)
	//Define the utility functions that are to be passed in the applications
	def toUser =  (line: Seq[String]) => User(line(0).toLong, line(1), line(2),line(3), line(4), line(5), line(6))
	def toFollow =  (line: Seq[String]) => Follow(line(0), line(1))
	def toMessage =  (line: Seq[String]) => Message(line(0), line(1).toLong, line(2), line(3).toLong)
	def viewsAndQueries()
	{
		//Load the user data into a Dataset
		val userDataDS = sc.textFile(dataDir + "user.txt").map(_.split("\\|")).map(toUser(_)).toDS()
		//Convert the Dataset into data frame
		val userDataDF = userDataDS.toDF()
		userDataDF.createOrReplaceTempView("user")
		userDataDF.show()
		//Load the follower data into an Dataset
		val followerDataDS = sc.textFile(dataDir + "follower.txt").map(_.split("\\|")).map(toFollow(_)).toDS()
		//Convert the Dataset into data frame
		val followerDataDF = followerDataDS.toDF()
		followerDataDF.createOrReplaceTempView("follow")
		followerDataDF.show()
		//Load the message data into an Dataset
		val messageDataDS = sc.textFile(dataDir + "message.txt").map(_.split("\\|")).map(toMessage(_)).toDS()
		//Convert the Dataset into data frame
		val messageDataDF = messageDataDS.toDF()
		messageDataDF.createOrReplaceTempView("message")
		messageDataDF.show()
		//Create the purposed view of the message to users
		val messagetoUsersDS = messageDataDS.filter(_.ShortMessage.contains("@")).map(message => (message.ShortMessage.split(" ").filter(_.contains("@")).mkString(" ").substring(1), message)).map(msgTuple => MessageToUsers(msgTuple._2.UserName, msgTuple._1, msgTuple._2.MessageId, msgTuple._2.ShortMessage, msgTuple._2.Timestamp))
		//Convert the Dataset into data frame
		val messagetoUsersDF = messagetoUsersDS.toDF()
		messagetoUsersDF.createOrReplaceTempView("messageToUsers")
		messagetoUsersDF.show()
		//Create the purposed view of tagged messages 
		val taggedMessageDS = messageDataDS.filter(_.ShortMessage.contains("#")).map(message => (message.ShortMessage.split(" ").filter(_.contains("#")).mkString(" "), message)).map(msgTuple => TaggedMessage(msgTuple._1, msgTuple._2.UserName, msgTuple._2.MessageId, msgTuple._2.ShortMessage, msgTuple._2.Timestamp))
		//Convert the Dataset into data frame
		val taggedMessageDF = taggedMessageDS.toDF()
		taggedMessageDF.createOrReplaceTempView("taggedMessages")
		taggedMessageDF.show()
		//The following are the queries given in the use cases
		//Find the messages that are grouped by a given hash tag
		val byHashTag = spark.sql("SELECT a.UserName, b.FirstName, b.LastName, a.MessageId, a.ShortMessage, a.Timestamp FROM taggedMessages a, user b WHERE a.UserName = b.UserName AND HashTag = '#Barcelona' ORDER BY a.Timestamp")
		byHashTag.show()
		//Find the messages that are addressed to a given user
		val byToUser = spark.sql("SELECT FromUserName, ToUserName, MessageId, ShortMessage, Timestamp FROM messageToUsers WHERE ToUserName = 'wbryson' ORDER BY Timestamp")
		byToUser.show()
		//Find the followers of a given user
		val followers = spark.sql("SELECT b.FirstName as FollowerFirstName, b.LastName as FollowerLastName, a.Followed FROM follow a, user b WHERE a.Follower = b.UserName AND a.Followed = 'wbryson'")
		followers.show()
		//Find the followedUsers of a given user
		val followedUsers = spark.sql("SELECT b.FirstName as FollowedFirstName, b.LastName as FollowedLastName, a.Follower FROM follow a, user b WHERE a.Followed = b.UserName AND a.Follower = 'eharris'")
		followedUsers.show()
	}
    def main(args: Array[String]) 
    { 
  	  viewsAndQueries()
    }	
}

