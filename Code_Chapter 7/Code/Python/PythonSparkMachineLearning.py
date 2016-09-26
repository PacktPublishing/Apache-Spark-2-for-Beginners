
# coding: utf-8

# In[32]:

print("==============Regression Use case==============")


# In[33]:

from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.ml.param import Param, Params
from pyspark.sql import Row


# In[34]:

# TODO - Change this directory to the right location where the data is stored
dataDir = "/Users/RajT/Downloads/wine-quality/"


# In[35]:

# Create the the RDD by reading the wine data from the disk 
lines = sc.textFile(dataDir + "winequality-red.csv")
splitLines = lines.map(lambda l: l.split(";"))
# Vector is a data type with 0 based indices and double-typed values. In that there are two types namely dense and sparse.
# A dense vector is backed by a double array representing its entry values, 
# A sparse vector is backed by two parallel arrays: indices and values
wineDataRDD = splitLines.map(lambda p: (float(p[11]), Vectors.dense([float(p[0]), float(p[1]), float(p[2]), float(p[3]), float(p[4]), float(p[5]), float(p[6]), float(p[7]), float(p[8]), float(p[9]), float(p[10])])))


# In[36]:

# Create the data frame containing the training data having two columns. 1) The actula output or label of the data 2) The vector containing the features
trainingDF = spark.createDataFrame(wineDataRDD, ['label', 'features'])
trainingDF.show()
# Create the object of the algorithm which is the Linear Regression with the parameters
# Linear regression parameter to make lr.fit() use at most 10 iterations
lr = LinearRegression(maxIter=10)
# Create a trained model by fitting the parameters using the training data
model = lr.fit(trainingDF)


# In[37]:

# Once the model is prepared, to test the model, prepare the test data containing the labels and feature vectors
testDF = spark.createDataFrame([
    (5.0, Vectors.dense([7.4, 0.7, 0.0, 1.9, 0.076, 25.0, 67.0, 0.9968, 3.2, 0.68,9.8])),
    (5.0, Vectors.dense([7.8, 0.88, 0.0, 2.6, 0.098, 11.0, 34.0, 0.9978, 3.51, 0.56, 9.4])),
    (7.0, Vectors.dense([7.3, 0.65, 0.0, 1.2, 0.065, 15.0, 18.0, 0.9968, 3.36, 0.57, 9.5]))], ["label", "features"])
testDF.createOrReplaceTempView("test")
testDF.show()


# In[38]:

# Do the transformation of the test data using the model and predict the output values or lables. This is to compare the predicted value and the actual label value
testTransform = model.transform(testDF)
tested = testTransform.select("features", "label", "prediction")
tested.show()


# In[39]:

# Prepare a data set without the output/lables to predict the output using the trained model
predictDF = spark.sql("SELECT features FROM test")
predictDF.show()


# In[40]:

# Do the transformation with the predict data set and display the predictions
predictTransform = model.transform(predictDF)
predicted = predictTransform.select("features", "prediction")
predicted.show()


# In[41]:

print("==============Classification Use case==============")


# In[42]:

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.param import Param, Params
from pyspark.sql import Row


# In[43]:

# TODO - Change this directory to the right location where the data is stored
dataDir = "/Users/RajT/Downloads/wine-quality/"


# In[44]:

# Create the the RDD by reading the wine data from the disk 
lines = sc.textFile(dataDir + "winequality-white.csv")
splitLines = lines.map(lambda l: l.split(";"))
wineDataRDD = splitLines.map(lambda p: (float(0) if (float(p[11]) < 7) else float(1), Vectors.dense([float(p[0]), float(p[1]), float(p[2]), float(p[3]), float(p[4]), float(p[5]), float(p[6]), float(p[7]), float(p[8]), float(p[9]), float(p[10])])))


# In[45]:

# Create the data frame containing the training data having two columns. 1) The actula output or label of the data 2) The vector containing the features
trainingDF = spark.createDataFrame(wineDataRDD, ['label', 'features'])
# Create the object of the algorithm which is the Logistic Regression with the parameters
# LogisticRegression parameter to make lr.fit() use at most 10 iterations and the regularization parameter.
# When a higher degree polynomial used by the algorithm to fit a set of points in a linear regression model, to prevent overfitting, regularization is used and this parameter is just for that
lr = LogisticRegression(maxIter=10, regParam=0.01)
# Create a trained model by fitting the parameters using the training data
model = lr.fit(trainingDF)
trainingDF.show()


# In[46]:

# Once the model is prepared, to test the model, prepare the test data containing the labels and feature vectors
testDF = spark.createDataFrame([
    (1.0, Vectors.dense([6.1,0.32,0.24,1.5,0.036,43,140,0.9894,3.36,0.64,10.7])),
    (0.0, Vectors.dense([5.2,0.44,0.04,1.4,0.036,38,124,0.9898,3.29,0.42,12.4])),
    (0.0, Vectors.dense([7.2,0.32,0.47,5.1,0.044,19,65,0.9951,3.38,0.36,9])),    
    (0.0, Vectors.dense([6.4,0.595,0.14,5.2,0.058,15,97,0.991,3.03,0.41,12.6]))], ["label", "features"])
testDF.createOrReplaceTempView("test")
testDF.show()


# In[47]:

# Do the transformation of the test data using the model and predict the output values or lables. This is to compare the predicted value and the actual label value
testTransform = model.transform(testDF)
tested = testTransform.select("features", "label", "prediction")
tested.show()


# In[48]:

# Prepare a data set without the output/lables to predict the output using the trained model
predictDF = spark.sql("SELECT features FROM test")
# Do the transformation with the predict data set and display the predictions
predictTransform = model.transform(predictDF)
predicted = testTransform.select("features", "prediction")
predicted.show()


# In[49]:

print("==============Spam Filtering Use case==============")


# In[50]:

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import Row


# In[51]:

# Prepare training documents from a list of messages from emails used to filter them as spam or not spam
# If the original message is a spam then the label is 1 and if the message is genuine then the label is 0
LabeledDocument = Row("email", "message", "label")
training = spark.createDataFrame([
    ("you@example.com", "hope you are well", 0.0),
    ("raj@example.com", "nice to hear from you", 0.0),
    ("thomas@example.com", "happy holidays", 0.0),
    ("mark@example.com", "see you tomorrow", 0.0),
    ("xyz@example.com", "save money", 1.0),  
    ("top10@example.com", "low interest rate", 1.0),
    ("marketing@example.com", "cheap loan", 1.0)], ["email", "message", "label"])


# In[52]:

training.show()


# In[53]:

# Configure an Spark machin learning pipeline, consisting of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="message", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features")
# LogisticRegression parameter to make lr.fit() use at most 10 iterations and the regularization parameter.
# When a higher degree polynomial used by the algorithm to fit a set of points in a linear regression model, to prevent overfitting, regularization is used and this parameter is just for that
lr = LogisticRegression(maxIter=10, regParam=0.01)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])
# Fit the pipeline to train the model to study the messages
model = pipeline.fit(training)


# In[54]:

# Prepare messages for prediction, which are not categorized and leaving upto the algorithm to predict
test = spark.createDataFrame([
    ("you@example.com", "how are you"),
    ("jain@example.com", "hope doing well"),
    ("caren@example.com", "want some money"),
    ("zhou@example.com", "secure loan"),
    ("ted@example.com","need loan")], ["email", "message"])
test.show()


# In[55]:

# Make predictions on the new messages
prediction = model.transform(test).select("email", "message", "prediction")
prediction.show()


# In[56]:

print("==============Finding Synonyms==============")


# In[57]:

from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql import Row


# In[58]:

# TODO - Change this directory to the right location where the data is stored
dataDir = "/Users/RajT/Downloads/20_newsgroups/*"
# Read the entire text into a DataFrame
textRDD = sc.wholeTextFiles(dataDir).map(lambda recs: Row(sentence=recs[1]))
textDF = spark.createDataFrame(textRDD)


# In[59]:

# Tokenize the sentences to words
regexTokenizer = RegexTokenizer(inputCol="sentence", outputCol="words", gaps=False, pattern="\\w+")
tokenizedDF = regexTokenizer.transform(textDF)


# In[60]:

# Prepare the Estimator
# It sets the vector size, and the parameter minCount sets the minimum number of times a token must appear to be included in the word2vec model's vocabulary.
word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="words", outputCol="result")
# Train the model
model = word2Vec.fit(tokenizedDF)


# In[61]:

# Find 10 synonyms of a given word
synonyms1 = model.findSynonyms("gun", 10)
synonyms1.show()


# In[62]:

# Find 10 synonyms of a different word
synonyms2 = model.findSynonyms("crime", 10)
synonyms2.show()


# In[ ]:



