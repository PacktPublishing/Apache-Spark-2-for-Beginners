IMPORTANT: Make sure that the $SPARK_HOME. $KAFKA_HOME environment variables are set properly pointing to your Spark and Kafka installations. Look at the TODO.txt in the lib directories to make sure that kafka pre-requisites are fulfilled.

The following script can be used to run the application in the local mode. Change the submitPy.sh file to suite your requirements

The Kafka streaming application requires Kafka jars to be shipped to the workers and hence while submitting the Spark job, those jar files are to be included in the job submission command. Since it is required only for the Kafka example, the second command line argument 1 is used to ship the required jar files.
The following script is used to run the data ingestion application
./submitPy.sh DataIngestionApp.py 1
For running the purposed view Python code, it is better to use IPython notebook. Otherwise the exported Python code file is available here and that has to be converted to have a main function and call that from there.