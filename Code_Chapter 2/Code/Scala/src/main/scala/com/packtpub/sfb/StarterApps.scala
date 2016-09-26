/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.StarterApps
**/


package com.packtpub.sfb

import org.apache.spark.sql.{Row, SparkSession}
class SparkStarter()
{
	// Create the Spark Session and the spark context				
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	val sc = spark.sparkContext	
	
	def gettingStarted()
	{
		// Creation of the list from where the RDD is going to be created
		val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30","SB10010,7000", "CR10001,7000", "SB10002,-10")
		// Create the RDD
		val acTransRDD = sc.parallelize(acTransList)
		// Apply filter and create another RDD of good transaction records
		val goodTransRecords = acTransRDD.filter(_.split(",")(1).toDouble > 0).filter(_.split(",")(0).startsWith("SB"))
		// Apply filter and create another RDD of high value transaction records
		val highValueTransRecords = goodTransRecords.filter(_.split(",")(1).toDouble > 1000)
		// The function that identifies the bad amounts
		val badAmountLambda = (trans: String) => trans.split(",")(1).toDouble <= 0
		// The function that identifies bad accounts
		val badAcNoLambda = (trans: String) => trans.split(",")(0).startsWith("SB") == false
		// Apply filter and create another RDD of bad amount records
		val badAmountRecords = acTransRDD.filter(badAmountLambda)
		// Apply filter and create another RDD of bad account records
		val badAccountRecords = acTransRDD.filter(badAcNoLambda)
		// Do the union of two RDDs and create another RDD
		val badTransRecords  = badAmountRecords.union(badAccountRecords)
		// Collect the values from the RDDs to the driver program
		acTransRDD.collect()
		goodTransRecords.collect()
		highValueTransRecords.collect()
		badAccountRecords.collect()
		badAmountRecords.collect()
		badTransRecords.collect()
		// Calculate the sum
		val sumAmount = goodTransRecords.map(trans => trans.split(",")(1).toDouble).reduce(_ + _)
		// Calculate the maximum
		val maxAmount = goodTransRecords.map(trans => trans.split(",")(1).toDouble).reduce((a, b) => if (a > b) a else b)
		// Calculate the minimum
		val minAmount = goodTransRecords.map(trans => trans.split(",")(1).toDouble).reduce((a, b) => if (a < b) a else b)
		// Combine all the elements
		val combineAllElements = acTransRDD.flatMap(trans => trans.split(","))
		// Find the good account numbers
		val allGoodAccountNos = combineAllElements.filter(_.startsWith("SB"))
		// Collect the values from the RDDs to the driver program
		combineAllElements.collect()
		allGoodAccountNos.distinct().collect()
	}
	def mapReduce()
	{
		// Creation of the list from where the RDD is going to be created
		val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10001,8000", "SB10002,400", "SB10003,300", "SB10001,10000", "SB10004,500", "SB10005,56", "SB10003,30","SB10002,7000", "SB10001,-100", "SB10002,-10")
		// Create the RDD
		val acTransRDD = sc.parallelize(acTransList)
		// Create the RDD containing key value pairs by doing mapping operation
		val acKeyVal = acTransRDD.map(trans => (trans.split(",")(0), trans.split(",")(1).toDouble))
		// Create the RDD by reducing key value pairs by doing applying sum operation to the values
		val accSummary = acKeyVal.reduceByKey(_ + _).sortByKey()
		// Collect the values from the RDDs to the driver program
		accSummary.collect()
		
		
	}
	def joins()
	{
		// Creation of the list from where the RDD is going to be created
		val acMasterList = Array("SB10001,Roger,Federer", "SB10002,Pete,Sampras", "SB10003,Rafel,Nadal", "SB10004,Boris,Becker", "SB10005,Ivan,Lendl")
		// Creation of the list from where the RDD is going to be created
		val acBalList = Array("SB10001,50000", "SB10002,12000", "SB10003,3000", "SB10004,8500", "SB10005,5000")
		// Create the RDD
		val acMasterRDD = sc.parallelize(acMasterList)
		// Create the RDD
		val acBalRDD = sc.parallelize(acBalList)
		// Create account master tuples
		val acMasterTuples = acMasterRDD.map(master => master.split(",")).map(masterList => (masterList(0), masterList(1) + " " + masterList(2)))
		// Create balance tuples
		val acBalTuples = acBalRDD.map(trans => trans.split(",")).map(transList => (transList(0), transList(1)))
		// Join the tuples	
		val acJoinTuples = acMasterTuples.join(acBalTuples).sortByKey().map{case (accno, (name, amount)) => (accno, name,amount)}
		// Collect the values to the driver program
		acJoinTuples.collect()
		// Find the account name and balance
		val acNameAndBalance = acJoinTuples.map{case (accno, name,amount) => (name,amount)}
		// Find the account tuples sorted by amount
		val acTuplesByAmount = acBalTuples.map{case (accno, amount) => (amount.toDouble, accno)}.sortByKey(false)
		// Get the top element
		acTuplesByAmount.first()
		// Get the top 3 elements
		acTuplesByAmount.take(3)
		// Count by the key
		acBalTuples.countByKey()
		// Count all the records
		acBalTuples.count()
		// Print the contents of the account name and balance RDD
		acNameAndBalance.foreach(println)
		// Find the balance total using accumulator
		val balanceTotal = sc.accumulator(0.0, "Account Balance Total")
		acBalTuples.map{case (accno, amount) => amount.toDouble}.foreach(bal => balanceTotal += bal)
		println(balanceTotal.value)
	}
	
	
	
}

object StarterApps {
  def main(args: Array[String]) 
  {
	  val sparkStarter = new SparkStarter()
	  sparkStarter.gettingStarted()
	  sparkStarter.mapReduce()
	  sparkStarter.joins()
  }
}