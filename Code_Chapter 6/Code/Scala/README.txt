IMPORTANT: Make sure that the $SPARK_HOME. $KAFKA_HOME environment variables are set properly pointing to your Spark and Kafka installations

The code can be compiled using the following wrapper script. Instead of this script, you may also use just the `sbt compile` command
./compile.sh



The following script can be used to run the application in the local mode. Change the submit.sh file to suite your requirements
IMPORTANT: The network client application must be running before running the below Spark application. The following command executed from a different terminal window will start that.
nc -lk 9999
./submit.sh com.packtpub.sfb.StreamingApps

The Kafka streaming application requires Kafka jars to be shipped to the workers and hence while submitting the Spark job, those jar files are to be included in the job submission command. Since it is required only for the Kafka example, the second command line argument 1 is used to ship the required jar files to Spark.
IMPORTANT: Before running the below applications, make sure that the Kafka broker and its producers are all running and producing messages
Run the following commands in one terminal window
cd $KAFKA_HOME
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

Run the following commands in another terminal window
cd $KAFKA_HOME
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

Make sure that the Kafka topic exists by running the following command in another terminal window.
cd $KAFKA_HOME
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sfb

Run the following commands in another terminal window
cd $KAFKA_HOME
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sfb

Run the following commands in another terminal window with the main Scala code directory as the current working directory
./submit.sh com.packtpub.sfb.KafkaStreamingApps 1