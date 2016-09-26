/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark. The second command line argument of value 1 is very important. This is to flag the shipping of the kafka jar files to the Spark cluster
./submit.sh com.packtpub.sfb.DataIngestionApp 1
**/
package com.packtpub.sfb

import java.util.HashMap
import org.apache.spark.streaming._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object DataIngestionApp {
  def main(args: Array[String]) {
	// Log level settings
	LogSettings.setLogLevels()
	//Check point directory for the recovery
	val checkPointDir = "/tmp"
    /**
    * The following function has to be used to have checkpointing and driver recovery
    * The way it should be used is to use the StreamingContext.getOrCreate with this function and do a start of that
	* This function example has been discussed but not used in the chapter covering Spark Streaming. But here it is being used
    */
    def sscCreateFn(): StreamingContext = {
    	// Variables used for creating the Kafka stream
  		// Zookeeper host
      	val zooKeeperQuorum = "localhost"
  		// Kaka message group
    	val messageGroup = "sfb-consumer-group"
  		// Kafka topic where the programming is listening for the data
		// Reader TODO: Here only one topic is included, it can take a comma separated string containing the list of topics. 
		// Reader TODO: When using multiple topics, use your own logic to extract the right message and persist to its data store
    	val topics = "message"
    	val numThreads = 1	  
  		// Create the Spark Session, the spark context and the streaming context		
  		val spark = SparkSession
  			.builder
  			.appName(getClass.getSimpleName)
  			.getOrCreate()
  		val sc = spark.sparkContext
      	val ssc = new StreamingContext(sc, Seconds(10))
      	val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      	val messageLines = KafkaUtils.createStream(ssc, zooKeeperQuorum, messageGroup, topicMap).map(_._2)
  		// This is where the messages are printed to the console. 
  		// TODO - As an exercise to the reader, instead of printing messages to the console, implement your own persistence logic
  		messageLines.print()
		//Do checkpointing for the recovery
    	ssc.checkpoint(checkPointDir)
		// return the Spark Streaming Context
  		ssc
    }
	// Note the function that is defined above for creating the Spark streaming context is being used here to create the Spark streaming context. 
	val ssc = StreamingContext.getOrCreate(checkPointDir, sscCreateFn)
	// Start the streaming
    ssc.start()
	// Wait till the application is terminated					
    ssc.awaitTermination()	
  }
  
}

object LogSettings {
  /** 
	Necessary log4j logging level settings are done 
  */
  def setLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
	  // This is to make sure that the console is clean from other INFO messages printed by Spark
	  Logger.getRootLogger.setLevel(Level.INFO)
    }
  }
}
