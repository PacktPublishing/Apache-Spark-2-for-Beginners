# The following script can be used to run this application in Spark
# ./submitPy.sh KafkaStreamingApps.py 1

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    # Create the Spark context
    sc = SparkContext(appName="PythonStreamingApp")
    # Necessary log4j logging level settings are done 
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
    # Create the Spark Streaming Context with 10 seconds batch interval
    ssc = StreamingContext(sc, 10)
    # Set the check point directory for saving the data to recover when there is a crash
    ssc.checkpoint("\tmp")
    # The quorum of Zookeeper hosts
    zooKeeperQuorum="localhost"
    # Message group name
    messageGroup="sfb-consumer-group"
    # Kafka topics list separated by coma if there are multiple topics to be listened on
    topics = "sfb"
    # Number of threads per topic
    numThreads = 1    
    # Create a Kafka DStream
    kafkaStream = KafkaUtils.createStream(ssc, zooKeeperQuorum, messageGroup, {topics: numThreads})
    # Create the Kafka stream
    appLogLines = kafkaStream.map(lambda x: x[1])
    # Count each log messge line containing the word ERROR
    errorLines = appLogLines.filter(lambda appLogLine: "ERROR" in appLogLine)
    # Print the first ten elements of each RDD generated in this DStream to the console
    errorLines.pprint()
    errorLines.countByWindow(30,10).pprint()
    # Start the streaming
    ssc.start()
    # Wait till the application is terminated	
    ssc.awaitTermination()
