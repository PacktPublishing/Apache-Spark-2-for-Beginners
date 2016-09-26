#!/usr/bin/env bash
#------------
# submitPy.sh
#------------
# IMPORTANT - Assumption is that the $SPARK_HOME and $KAFKA_HOME environment variables are already set in the system that is running the application

# Disable randomized hash in Python 3.3+ (for string) Otherwise the following exception will occur
# raise Exception("Randomness of hash of string should be disabled via PYTHONHASHSEED")
# Exception: Randomness of hash of string should be disabled via PYTHONHASHSEED
export PYTHONHASHSEED=0

# [FILLUP] Which is your Spark master. If monitoring is needed, use the desired Spark master or use local
# When using the local mode. It is important to give more than one cores in square brackets
#SPARK_MASTER=spark://Rajanarayanans-MacBook-Pro.local:7077
SPARK_MASTER=local[4]

# [OPTIONAL] Pass the application name to run as the parameter to this script
APP_TO_RUN=$1

# [OPTIONAL] Spark submit command
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

if [ $2 -eq 1 ]
then
  $SPARK_SUBMIT --master $SPARK_MASTER --jars $KAFKA_HOME/libs/kafka-clients-0.8.2.2.jar,$KAFKA_HOME/libs/kafka_2.11-0.8.2.2.jar,$KAFKA_HOME/libs/metrics-core-2.2.0.jar,$KAFKA_HOME/libs/zkclient-0.3.jar,./lib/spark-streaming-kafka-0-8_2.11-2.0.0-preview.jar $APP_TO_RUN
else
  $SPARK_SUBMIT --master $SPARK_MASTER $APP_TO_RUN
fi