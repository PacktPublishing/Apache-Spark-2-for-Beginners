# The following script can be used to run this application in Spark
# ./submitPy.sh DataIngestionApp.py 1

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    # Create the Spark context
    sc = SparkContext(appName="DataIngestionApp")
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
    # Create the Spark Streaming Context with 10 seconds batch interval
    ssc = StreamingContext(sc, 10)
    # Check point directory setting
    ssc.checkpoint("\tmp")
    # Zookeeper host
    zooKeeperQuorum="localhost"
    # Kaka message group
    messageGroup="sfb-consumer-group"
    # Kafka topic where the programming is listening for the data
	# Reader TODO: Here only one topic is included, it can take a comma separated string containing the list of topics. 
	# Reader TODO: When using multiple topics, use your own logic to extract the right message and persist to its data store
    topics = "message"
    numThreads = 1    
    # Create a Kafka DStream
    kafkaStream = KafkaUtils.createStream(ssc, zooKeeperQuorum, messageGroup, {topics: numThreads})
    messageLines = kafkaStream.map(lambda x: x[1])
    # This is where the messages are printed to the console. Instead of this, implement your own persistence logic
    messageLines.pprint()
    # Start the streaming
    ssc.start()
    # Wait till the application is terminated	
    ssc.awaitTermination()
