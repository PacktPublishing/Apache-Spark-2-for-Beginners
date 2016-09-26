IMPORTANT: Make sure that the $SPARK_HOME. $KAFKA_HOME environment variables are set properly pointing to your Spark and Kafka installations. Look at the TODO.txt in the lib directories to make sure that kafka pre-requisites are fulfilled.

The code can be compiled using the following wrapper script. Instead of this script, you may also use just the `sbt compile` command
./compile.sh

The following scripts can be used to run the application in the local mode. Change the submit.sh file to suite your requirements
The following script is used to run the data ingestion application
./submit.sh com.packtpub.sfb.DataIngestionApp 1
The following script is used to create the purposed views and sample queries
./submit.sh com.packtpub.sfb.PurposedViews
The following script is used to create the purposed views using cusom data processing such as GraphX
./submit.sh com.packtpub.sfb.ConnectionApp


