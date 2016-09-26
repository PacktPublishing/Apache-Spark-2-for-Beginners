/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.SparkSQLStarterApps
**/


package com.packtpub.sfb

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object SparkSQLStarterApps
{
	// Create the Spark Session and the spark context				
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	val sc = spark.sparkContext
	import spark.implicits._
	// Define the case classes for using in conjunction with DataFrames
	case class Trans(accNo: String, tranAmount: Double)
	case class AcMaster(accNo: String, firstName: String, lastName: String)
	case class AcBal(accNo: String, balanceAmount: Double)
	// Functions to convert the sequence of strings to objects defined by the case classes
	def toTrans =  (trans: Seq[String]) => Trans(trans(0), trans(1).trim.toDouble)
	def toAcMaster =  (master: Seq[String]) => AcMaster(master(0), master(1), master(2))
	def toAcBal =  (bal: Seq[String]) => AcBal(bal(0), bal(1).trim.toDouble)
	def gettingStarted()
	{
		// Creation of the list from where the RDD is going to be created
		val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30","SB10010,7000", "CR10001,7000", "SB10002,-10")
		// Create the RDD
		val acTransRDD = sc.parallelize(acTransList).map(_.split(",")).map(toTrans(_))
		// Convert RDD to DataFrame
		
		val acTransDF = spark.createDataFrame(acTransRDD)
		// Register temporary view in the DataFrame for using it in SQL
		acTransDF.createOrReplaceTempView("trans")
		// Print the structure of the DataFrame
		acTransDF.printSchema
		// Show the first few records of the DataFrame
		acTransDF.show
		// Use SQL to create another DataFrame containing the good transaction records
		val goodTransRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE accNo like 'SB%' AND tranAmount > 0")
		// Register temporary view in the DataFrame for using it in SQL
		goodTransRecords.createOrReplaceTempView("goodtrans")
		// Show the first few records of the DataFrame
		goodTransRecords.show
		// Use SQL to create another DataFrame containing the high value transaction records
		val highValueTransRecords = spark.sql("SELECT accNo, tranAmount FROM goodtrans WHERE tranAmount > 1000")
		// Show the first few records of the DataFrame
		highValueTransRecords.show
		// Use SQL to create another DataFrame containing the bad account records
		val badAccountRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE accNo NOT like 'SB%'")
		// Show the first few records of the DataFrame
		badAccountRecords.show
		// Use SQL to create another DataFrame containing the bad amount records
		val badAmountRecords = spark.sql("SELECT accNo, tranAmount FROM trans WHERE tranAmount < 0")
		// Show the first few records of the DataFrame
		badAmountRecords.show
		// Do the union of two DataFrames and create another DataFrame
		val badTransRecords = badAccountRecords.union(badAmountRecords)
		// Show the first few records of the DataFrame
		badTransRecords.show
		// Calculate the sum
		val sumAmount = spark.sql("SELECT sum(tranAmount) as sum FROM goodtrans")
		// Show the first few records of the DataFrame
		sumAmount.show
		// Calculate the maximum
		val maxAmount = spark.sql("SELECT max(tranAmount) as max FROM goodtrans")
		// Show the first few records of the DataFrame
		maxAmount.show
		// Calculate the minimum
		val minAmount = spark.sql("SELECT min(tranAmount) as min FROM goodtrans")
		// Show the first few records of the DataFrame
		minAmount.show
		// Use SQL to create another DataFrame containing the good account numbers
		val goodAccNos = spark.sql("SELECT DISTINCT accNo FROM trans WHERE accNo like 'SB%' ORDER BY accNo")
		// Show the first few records of the DataFrame
		goodAccNos.show
		// Calculate the aggregates using mixing of DataFrame and RDD like operations
		val sumAmountByMixing = goodTransRecords.map(trans => trans.getAs[Double]("tranAmount")).reduce(_ + _)
		val maxAmountByMixing = goodTransRecords.map(trans => trans.getAs[Double]("tranAmount")).reduce((a, b) => if (a > b) a else b)
		val minAmountByMixing = goodTransRecords.map(trans => trans.getAs[Double]("tranAmount")).reduce((a, b) => if (a < b) a else b)
	}
	def gettingStartedAPI()
	{
		// Creation of the list from where the RDD is going to be created
		val acTransList = Array("SB10001,1000", "SB10002,1200", "SB10003,8000", "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500", "SB10008,56", "SB10009,30","SB10010,7000", "CR10001,7000", "SB10002,-10")
		// Create the DataFrame
		val acTransDF = sc.parallelize(acTransList).map(_.split(",")).map(toTrans(_)).toDF()
		// Create the DataFrame using API for the good transaction records
		val goodTransRecords = acTransDF.filter("accNo like 'SB%'").filter("tranAmount > 0")
		// Show the first few records of the DataFrame
		goodTransRecords.show
		// Create the DataFrame using API for the high value transaction records
		val highValueTransRecords = goodTransRecords.filter("tranAmount > 1000")
		// Show the first few records of the DataFrame
		highValueTransRecords.show
		// Create the DataFrame using API for the bad account records
		val badAccountRecords = acTransDF.filter("accNo NOT like 'SB%'")
		// Show the first few records of the DataFrame
		badAccountRecords.show
		// Create the DataFrame using API for the bad amount records
		val badAmountRecords = acTransDF.filter("tranAmount < 0")
		// Show the first few records of the DataFrame
		badAmountRecords.show
		// Do the union of two DataFrames and create another DataFrame
		val badTransRecords = badAccountRecords.union(badAmountRecords)
		// Show the first few records of the DataFrame
		badTransRecords.show
		// Calculate the aggregates in one shot
		val aggregates = goodTransRecords.agg(sum("tranAmount"), max("tranAmount"), min("tranAmount"))
		// Show the first few records of the DataFrame
		aggregates.show
		// Create the DataFrame using API for the good account numbers
		val goodAccNos = acTransDF.filter("accNo like 'SB%'").select("accNo").distinct().orderBy("accNo")
		// Show the first few records of the DataFrame
		goodAccNos.show
		// Persist the data of the DataFrame into a Parquet file
		acTransDF.write.parquet("scala.trans.parquet")
		// Read the data into a DataFrame from the Parquet file
		val acTransDFfromParquet = spark.read.parquet("scala.trans.parquet")
		// Show the first few records of the DataFrame
		acTransDFfromParquet.show
		
		
		
	}
	def aggregations()
	{
		// Creation of the list from where the RDD is going to be created
		val acTransList = Array("SB10001,1000", "SB10002,1200","SB10001,8000", "SB10002,400", "SB10003,300", "SB10001,10000","SB10004,500","SB10005,56", "SB10003,30","SB10002,7000","SB10001,-100", "SB10002,-10")
		// Create the DataFrame
		val acTransDF = sc.parallelize(acTransList).map(_.split(",")).map(toTrans(_)).toDF()
		// Show the first few records of the DataFrame
		acTransDF.show
		// Register temporary view in the DataFrame for using it in SQL
		acTransDF.createOrReplaceTempView("trans")
		// Use SQL to create another DataFrame containing the account summary records
		val acSummary = spark.sql("SELECT accNo, sum(tranAmount) as TransTotal FROM trans GROUP BY accNo")
		// Show the first few records of the DataFrame
		acSummary.show
		// Create the DataFrame using API for the account summary records
		val acSummaryViaDFAPI = acTransDF.groupBy("accNo").agg(sum("tranAmount") as "TransTotal")
		// Show the first few records of the DataFrame
		acSummaryViaDFAPI.show
		
	}
	def multiDataSourceJoins()
	{
		// Creation of the list from where the RDD is going to be created
		val acMasterList = Array("SB10001,Roger,Federer","SB10002,Pete,Sampras", "SB10003,Rafael,Nadal","SB10004,Boris,Becker", "SB10005,Ivan,Lendl")
		// Creation of the list from where the RDD is going to be created
		val acBalList = Array("SB10001,50000", "SB10002,12000","SB10003,3000", "SB10004,8500", "SB10005,5000")
		// Create the DataFrame
		val acMasterDF = sc.parallelize(acMasterList).map(_.split(",")).map(toAcMaster(_)).toDF()
		// Create the DataFrame
		val acBalDF = sc.parallelize(acBalList).map(_.split(",")).map(toAcBal(_)).toDF()
		// Persist the data of the DataFrame into a Parquet file
		acMasterDF.write.parquet("scala.master.parquet")
		// Persist the data of the DataFrame into a JSON file
		acBalDF.write.json("scalaMaster.json")
		// Read the data into a DataFrame from the Parquet file
		val acMasterDFFromFile = spark.read.parquet("scala.master.parquet")
		// Register temporary view in the DataFrame for using it in SQL
		acMasterDFFromFile.createOrReplaceTempView("master")
		// Read the data into a DataFrame from the JSON file
		val acBalDFFromFile = spark.read.json("scalaMaster.json")
		// Register temporary view in the DataFrame for using it in SQL
		acBalDFFromFile.createOrReplaceTempView("balance")
		// Show the first few records of the DataFrame
		acMasterDFFromFile.show
		acBalDFFromFile.show
		// Use SQL to create another DataFrame containing the account detail records
		val acDetail = spark.sql("SELECT master.accNo, firstName, lastName, balanceAmount FROM master, balance WHERE master.accNo = balance.accNo ORDER BY balanceAmount DESC")
		// Show the first few records of the DataFrame
		acDetail.show
		// Create the DataFrame using API for the account detail records
		val acDetailFromAPI = acMasterDFFromFile.join(acBalDFFromFile, acMasterDFFromFile("accNo") === acBalDFFromFile("accNo"), "inner").sort($"balanceAmount".desc).select(acMasterDFFromFile("accNo"), acMasterDFFromFile("firstName"), acMasterDFFromFile("lastName"), acBalDFFromFile("balanceAmount"))
		// Show the first few records of the DataFrame
		acDetailFromAPI.show
		// Use SQL to create another DataFrame containing the top 3 account detail records
		val acDetailTop3 = spark.sql("SELECT master.accNo, firstName, lastName, balanceAmount FROM master, balance WHERE master.accNo = balance.accNo ORDER BY balanceAmount DESC").limit(3)
		// Show the first few records of the DataFrame
		acDetailTop3.show
	}
    def main(args: Array[String]) 
    { 
  	  gettingStarted()
  	  gettingStartedAPI()
  	  aggregations()
  	  multiDataSourceJoins()
    }	
}

