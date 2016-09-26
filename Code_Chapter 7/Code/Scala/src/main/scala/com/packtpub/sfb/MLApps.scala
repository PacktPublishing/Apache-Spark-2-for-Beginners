/**
The following program can be compiled and run using SBT
Wrapper scripts have been provided with this
The following script can be run to compile the code
./compile.sh

The following script can be used to run this application in Spark
./submit.sh com.packtpub.sfb.MLApps
**/

package com.packtpub.sfb
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{HashingTF, Tokenizer, RegexTokenizer, Word2Vec, StopWordsRemover}

object MLApps {
	// Create the Spark Session and the spark context				
  val spark = SparkSession
			.builder
			.appName(getClass.getSimpleName)
			.getOrCreate()
  val sc = spark.sparkContext	
  import spark.implicits._	
  def main(args: Array[String]) {
	  linearRegression()
	  logisticRegression()
	  multiStagePipeline()
	  synonymFinder()
  }
  def linearRegression(){
	  // TODO - Change this directory to the right location where the data is stored
	  val dataDir = "/Users/RajT/Downloads/wine-quality/"
	  // Define the case class that holds the wine data
	  case class Wine(FixedAcidity: Double, VolatileAcidity: Double, CitricAcid: Double, ResidualSugar: Double, Chlorides: Double, FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH: Double, Sulphates: Double, Alcohol: Double, Quality: Double)
	  // Create the the RDD by reading the wine data from the disk 
	  //TODO - The wine data has to be downloaded to the appropriate working directory in the system where this is being run and the following line of code should use that path
	  val wineDataRDD = sc.textFile(dataDir + "winequality-red.csv").map(_.split(";")).map(w => Wine(w(0).toDouble, w(1).toDouble, w(2).toDouble, w(3).toDouble, w(4).toDouble, w(5).toDouble, w(6).toDouble, w(7).toDouble, w(8).toDouble, w(9).toDouble, w(10).toDouble, w(11).toDouble))
	  // Create the data frame containing the training data having two columns. 1) The actula output or label of the data 2) The vector containing the features
	  //Vector is a data type with 0 based indices and double-typed values. In that there are two types namely dense and sparse.
	  //A dense vector is backed by a double array representing its entry values, 
	  //A sparse vector is backed by two parallel arrays: indices and values
	  val trainingDF = wineDataRDD.map(w => (w.Quality, Vectors.dense(w.FixedAcidity, w.VolatileAcidity, w.CitricAcid, w.ResidualSugar, w.Chlorides, w.FreeSulfurDioxide, w.TotalSulfurDioxide, w.Density, w.PH, w.Sulphates, w.Alcohol))).toDF("label", "features")
	  trainingDF.show()
	  // Create the object of the algorithm which is the Linear Regression
	  val lr = new LinearRegression()
	  // Linear regression parameter to make lr.fit() use at most 10 iterations
	  lr.setMaxIter(10)
	  // Create a trained model by fitting the parameters using the training data
	  val model = lr.fit(trainingDF)
	  // Once the model is prepared, to test the model, prepare the test data containing the labels and feature vectors
	  val testDF = spark.createDataFrame(Seq(
  		(5.0, Vectors.dense(7.4, 0.7, 0.0, 1.9, 0.076, 25.0, 67.0, 0.9968, 3.2, 0.68,9.8)),
  	  	(5.0, Vectors.dense(7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0, 0.9978, 3.51, 0.56, 9.4)),
  	  	(7.0, Vectors.dense(7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0, 0.9968, 3.36, 0.57, 9.5))
		)).toDF("label", "features")
		testDF.show()
	  testDF.createOrReplaceTempView("test")
	  // Do the transformation of the test data using the model and predict the output values or lables. This is to compare the predicted value and the actual label value
	  val tested = model.transform(testDF).select("features", "label", "prediction")
	  tested.show()
	  // Prepare a data set without the output/lables to predict the output using the trained model
	  val predictDF = spark.sql("SELECT features FROM test")
	  // Do the transformation with the predict data set and display the predictions
	  val predicted = model.transform(predictDF).select("features", "prediction")
	  predicted.show()
  }
  def logisticRegression(){
	// TODO - Change this directory to the right location where the data is stored
  	val dataDir = "/Users/RajT/Downloads/wine-quality/"
  	// Define the case class that holds the wine data
	case class Wine(FixedAcidity: Double, VolatileAcidity: Double, CitricAcid: Double, ResidualSugar: Double, Chlorides: Double, FreeSulfurDioxide: Double, TotalSulfurDioxide: Double, Density: Double, PH: Double, Sulphates: Double, Alcohol: Double, Quality: Double)
	// Create the the RDD by reading the wine data from the disk 
	val wineDataRDD = sc.textFile(dataDir + "winequality-white.csv").map(_.split(";")).map(w => Wine(w(0).toDouble, w(1).toDouble, w(2).toDouble, w(3).toDouble, w(4).toDouble, w(5).toDouble, w(6).toDouble, w(7).toDouble, w(8).toDouble, w(9).toDouble, w(10).toDouble, w(11).toDouble))
	// Create the data frame containing the training data having two columns. 1) The actula output or label of the data 2) The vector containing the features
	val trainingDF = wineDataRDD.map(w => (if(w.Quality < 7) 0D else 1D, Vectors.dense(w.FixedAcidity, w.VolatileAcidity, w.CitricAcid, w.ResidualSugar, w.Chlorides, w.FreeSulfurDioxide, w.TotalSulfurDioxide, w.Density, w.PH, w.Sulphates, w.Alcohol))).toDF("label", "features")
	trainingDF.show()
	// Create the object of the algorithm which is the Logistic Regression
	val lr = new LogisticRegression()
	// LogisticRegression parameter to make lr.fit() use at most 10 iterations and the regularization parameter.
	// When a higher degree polynomial used by the algorithm to fit a set of points in a linear regression model, to prevent overfitting, regularization is used and this parameter is just for that
	lr.setMaxIter(10).setRegParam(0.01)
	// Create a trained model by fitting the parameters using the training data
	val model = lr.fit(trainingDF)
	// Once the model is prepared, to test the model, prepare the test data containing the labels and feature vectors
	val testDF = spark.createDataFrame(Seq(
    (1.0, Vectors.dense(6.1,0.32,0.24,1.5,0.036,43,140,0.9894,3.36,0.64,10.7)),
    (0.0, Vectors.dense(5.2,0.44,0.04,1.4,0.036,38,124,0.9898,3.29,0.42,12.4)),
    (0.0, Vectors.dense(7.2,0.32,0.47,5.1,0.044,19,65,0.9951,3.38,0.36,9)),    
    (0.0, Vectors.dense(6.4,0.595,0.14,5.2,0.058,15,97,0.991,3.03,0.41,12.6))
	)).toDF("label", "features")
	testDF.show()
	testDF.createOrReplaceTempView("test")
	// Do the transformation of the test data using the model and predict the output values or lables. This is to compare the predicted value and the actual label value
	val tested = model.transform(testDF).select("features", "label", "prediction")
	tested.show()
	// Prepare a data set without the output/lables to predict the output using the trained model
	val predictDF = spark.sql("SELECT features FROM test")
	// Do the transformation with the predict data set and display the predictions
	val predicted = model.transform(predictDF).select("features", "prediction")
	predicted.show()
  }
  
  def multiStagePipeline(){
	  // Prepare training documents from a list of messages from emails used to filter them as spam or not spam
	  // If the original message is a spam then the label is 1 and if the message is genuine then the label is 0
	  val training = spark.createDataFrame(Seq(
	    ("you@example.com", "hope you are well", 0.0),
	    ("raj@example.com", "nice to hear from you", 0.0),
	    ("thomas@example.com", "happy holidays", 0.0),
	    ("mark@example.com", "see you tomorrow", 0.0),
	    ("xyz@example.com", "save money", 1.0),
	    ("top10@example.com", "low interest rate", 1.0),
	    ("marketing@example.com", "cheap loan", 1.0)
	  )).toDF("email", "message", "label")
	  training.show()

	  // Configure an Spark machine learning pipeline, consisting of three stages: tokenizer, hashingTF, and lr.
	  val tokenizer = new Tokenizer().setInputCol("message").setOutputCol("words")
	  val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("features")
  	  // LogisticRegression parameter to make lr.fit() use at most 10 iterations and the regularization parameter.
  	  // When a higher degree polynomial used by the algorithm to fit a set of points in a linear regression model, to prevent overfitting, regularization is used and this parameter is just for that
	  val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
	  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

	  // Fit the pipeline to train the model to study the messages
	  val model = pipeline.fit(training)

	  // Prepare messages for prediction, which are not categorized and leaving upto the algorithm to predict
	  val test = spark.createDataFrame(Seq(
	    ("you@example.com", "how are you"),
	    ("jain@example.com", "hope doing well"),
	    ("caren@example.com", "want some money"),
	    ("zhou@example.com", "secure loan"),
	    ("ted@example.com","need loan")
	  )).toDF("email", "message")
	  test.show()

	  // Make predictions on the new messages
	  val prediction = model.transform(test).select("email", "message", "prediction")
	  prediction.show()
  	
  }
  
  def synonymFinder(){
	  // TODO - Change this directory to the right location where the data is stored
	  val dataDir = "/Users/RajT/Downloads/20_newsgroups/*"
	  //Read the entire text into a DataFrame
	  val textDF = sc.wholeTextFiles(dataDir).map{case(file, text) => text}.map(Tuple1.apply).toDF("sentence")
	  // Tokenize the sentences to words
	  val regexTokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("\\w+").setGaps(false)
	  val tokenizedDF = regexTokenizer.transform(textDF)
	  // Remove the stop words such as a, an the, I etc which doesn't have any specific relevance to the synonyms
	  val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered")
	  //Remove the stop words from the text
	  val filteredDF = remover.transform(tokenizedDF)
	  //Prepare the Estimator
	  //It sets the vector size, and the method setMinCount sets the minimum number of times a token must appear to be included in the word2vec model's vocabulary.
	  val word2Vec = new Word2Vec().setInputCol("filtered").setOutputCol("result").setVectorSize(3).setMinCount(0)
	  //Train the model
	  val model = word2Vec.fit(filteredDF)
	  //Find 10 synonyms of a given word
	  val synonyms1 = model.findSynonyms("gun", 10)
	  synonyms1.show()
	  //Find 10 synonyms of a different word
	  val synonyms2 = model.findSynonyms("crime", 10)
	  synonyms2.show()
  }
}




