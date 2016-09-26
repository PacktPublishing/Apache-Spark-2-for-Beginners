#!/bin/bash
#-----------
# submit.sh
#-----------
# IMPORTANT - Assumption is that the $SPARK_HOME environment variables is already set in the system that is running the application

# [OPTIONAL] Which is your Spark master 
SPARK_MASTER=local

# [OPTIONAL] Your Scala version
SCALA_VERSION="2.11"

# Name of the application jar file. You should be OK to leave it like that
APP_JAR="spark-for-beginners_$SCALA_VERSION-1.0.jar"

# Absolute path to the application jar file
PATH_TO_APP_JAR="target/scala-$SCALA_VERSION/$APP_JAR"

# Spark submit command
SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"

# Pass the application name to run as the parameter to this script
APP_TO_RUN=$1

sbt package
$SPARK_SUBMIT --class $APP_TO_RUN --master $SPARK_MASTER --jars $PATH_TO_APP_JAR $PATH_TO_APP_JAR