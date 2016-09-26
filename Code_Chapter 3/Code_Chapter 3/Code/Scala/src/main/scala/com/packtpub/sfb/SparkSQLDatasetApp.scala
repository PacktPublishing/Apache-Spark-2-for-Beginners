/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.SparkSQLDatasetApp
**/


package com.packtpub.sfb

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoder

object SparkSQLDatasetApp
{
	// Create the Spark Session and the spark context				
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	val sc = spark.sparkContext	
	import spark.implicits._
	
	// Define the case classes for using in conjunction with DataFrames and Dataset
	case class Trans(accNo: String, tranAmount: Double) 
    def main(args: Array[String]) 
    {
		// Creation of the list from where the Dataset is going to be created using a case class.
		val acTransList = Seq(Trans("SB10001", 1000), Trans("SB10002",1200), Trans("SB10003", 8000), Trans("SB10004",400), Trans("SB10005",300), Trans("SB10006",10000), Trans("SB10007",500), Trans("SB10008",56), Trans("SB10009",30),Trans("SB10010",7000), Trans("CR10001",7000), Trans("SB10002",-10))
		// Create the Dataset
		val acTransDS = acTransList.toDS()
		acTransDS.show()
		// Apply filter and create another Dataset of good transaction records
		val goodTransRecords = acTransDS.filter(_.tranAmount > 0).filter(_.accNo.startsWith("SB"))
		goodTransRecords.show()
		// Apply filter and create another Dataset of high value transaction records
		val highValueTransRecords = goodTransRecords.filter(_.tranAmount > 1000)
		highValueTransRecords.show()
		// The function that identifies the bad amounts
		val badAmountLambda = (trans: Trans) => trans.tranAmount <= 0
		// The function that identifies bad accounts
		val badAcNoLambda = (trans: Trans) => trans.accNo.startsWith("SB") == false
		// Apply filter and create another Dataset of bad amount records
		val badAmountRecords = acTransDS.filter(badAmountLambda)
		badAmountRecords.show()
		// Apply filter and create another Dataset of bad account records
		val badAccountRecords = acTransDS.filter(badAcNoLambda)
		badAccountRecords.show()
		// Do the union of two Dataset and create another Dataset
		val badTransRecords  = badAmountRecords.union(badAccountRecords)
		badTransRecords.show()
		// Calculate the sum
		val sumAmount = goodTransRecords.map(trans => trans.tranAmount).reduce(_ + _)
		// Calculate the maximum
		val maxAmount = goodTransRecords.map(trans => trans.tranAmount).reduce((a, b) => if (a > b) a else b)
		// Calculate the minimum
		val minAmount = goodTransRecords.map(trans => trans.tranAmount).reduce((a, b) => if (a < b) a else b)
		// Convert the Dataset to DataFrame
		val acTransDF = acTransDS.toDF()
		acTransDF.show()
		// Use Spark SQL to find out invalid transaction records
		acTransDF.createOrReplaceTempView("trans")
		val invalidTransactions = spark.sql("SELECT accNo, tranAmount FROM trans WHERE (accNo NOT LIKE 'SB%') OR tranAmount <= 0")
		invalidTransactions.show()
		// Interoperability of RDD, DataFrame and Dataset
		// Create RDD
		val acTransRDD = sc.parallelize(acTransList)
		// Convert RDD to DataFrame
		val acTransRDDtoDF = acTransRDD.toDF()
		// Convert the DataFrame to Dataset with the type checking
		val acTransDFtoDS = acTransRDDtoDF.as[Trans]
		acTransDFtoDS.show()
    }	
}

