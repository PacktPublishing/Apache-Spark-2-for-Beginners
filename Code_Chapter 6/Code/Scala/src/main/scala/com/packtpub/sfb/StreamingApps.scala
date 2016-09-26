/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.StreamingApps
**/
package com.packtpub.sfb

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}


object StreamingApps{
  def main(args: Array[String]) 
  {
	// Log level settings
	LogSettings.setLogLevels()
	// Create the Spark Session and the spark context				
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	// Get the Spark context from the Spark session for creating the streaming context
	val sc = spark.sparkContext	
  	// Create the streaming context
	val ssc = new StreamingContext(sc, Seconds(10))
	// Set the check point directory for saving the data to recover when there is a crash
	ssc.checkpoint("/tmp")
	println("Stream processing logic start")
	// Create a DStream that connects to localhost on port 9999
	// The StorageLevel.MEMORY_AND_DISK_SER indicates that the data will be stored in memory and if it overflows, in disk as well
	val appLogLines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
	// Count each log message line containing the word ERROR
	val errorLines = appLogLines.filter(line => line.contains("ERROR"))
	// Print the elements of each RDD generated in this DStream to the console
	errorLines.print()
	// Count the number of messages by the windows and print them
	errorLines.countByWindow(Seconds(30), Seconds(10)).print()
	println("Stream processing logic end")
	// Start the streaming
	ssc.start()   
	// Wait till the application is terminated	          
	ssc.awaitTermination()  
  }
}


object LogSettings{
  /** 
	Necessary log4j logging level settings are done 
  */
  def setLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
	    // This is to make sure that the console is clean from other INFO messages printed by Spark
	    Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}