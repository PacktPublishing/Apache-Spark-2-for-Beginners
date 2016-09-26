/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark. The second command line argument of value 1 is very important. This is to flag the shipping of the kafka jar files to the Spark cluster
./submit.sh com.packtpub.sfb.KafkaStreamingApps 1
**/
package com.packtpub.sfb

import java.util.HashMap
import org.apache.spark.streaming._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

object KafkaStreamingApps {
  def main(args: Array[String]) {
	// Log level settings
	LogSettings.setLogLevels()
	// Variables used for creating the Kafka stream
	//The quorum of Zookeeper hosts
    val zooKeeperQuorum = "localhost"
	// Message group name
	val messageGroup = "sfb-consumer-group"
	//Kafka topics list separated by coma if there are multiple topics to be listened on
	val topics = "sfb"
	//Number of threads per topic
	val numThreads = 1
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
	// Create the map of topic names
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
	// Create the Kafka stream
    val appLogLines = KafkaUtils.createStream(ssc, zooKeeperQuorum, messageGroup, topicMap).map(_._2)
	// Count each log messge line containing the word ERROR
    val errorLines = appLogLines.filter(line => line.contains("ERROR"))
	// Print the line containing the error
	errorLines.print()
	// Count the number of messages by the windows and print them
	errorLines.countByWindow(Seconds(30), Seconds(10)).print()
	// Start the streaming
    ssc.start()	
	// Wait till the application is terminated				
    ssc.awaitTermination()	
  }
  
  
  /**
  * The following function has to be used when the code is being restructured to have checkpointing and driver recovery
  * The way it should be used is to use the StreamingContext.getOrCreate with this function and do a start of that
  */
  def sscCreateFn(): StreamingContext = {
  	// Variables used for creating the Kafka stream
	// The quorum of Zookeeper hosts
    val zooKeeperQuorum = "localhost"
	// Message group name
  	val messageGroup = "sfb-consumer-group"
	//Kafka topics list separated by coma if there are multiple topics to be listened on
  	val topics = "sfb"
	//Number of threads per topic
  	val numThreads = 1	  
	// Create the Spark Session and the spark context				
	val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
	// Get the Spark context from the Spark session for creating the streaming context
	val sc = spark.sparkContext	
	// Create the streaming context
  	val ssc = new StreamingContext(sc, Seconds(10))
	// Create the map of topic names
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
	// Create the Kafka stream
    val appLogLines = KafkaUtils.createStream(ssc, zooKeeperQuorum, messageGroup, topicMap).map(_._2)
	// Count each log messge line containing the word ERROR
    val errorLines = appLogLines.filter(line => line.contains("ERROR"))
	// Print the line containing the error
	errorLines.print()
	// Count the number of messages by the windows and print them
	errorLines.countByWindow(Seconds(30), Seconds(10)).print()
	// Set the check point directory for saving the data to recover when there is a crash
  	ssc.checkpoint("/tmp")
	// Return the streaming context
	ssc
  }
  
  
}
