# The following script can be used to run this application in Spark
# ./submitPy.sh StreamingApps.py

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

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
    # Create a DStream that connects to localhost on port 9999
    appLogLines = ssc.socketTextStream("localhost", 9999)
    # Count each log messge line containing the word ERROR
    errorLines = appLogLines.filter(lambda appLogLine: "ERROR" in appLogLine)
    # // Print the elements of each RDD generated in this DStream to the console
    errorLines.pprint()
    # Count the number of messages by the windows and print them
    errorLines.countByWindow(30,10).pprint()
    # Start the streaming
    ssc.start()
    # Wait till the application is terminated	
    ssc.awaitTermination()
