# Spark_Pyspark
The first step to using Spark is connecting to a cluster.
To create one connection we should create an instance of the SparkContext (sc) class.
SparkConf()

SparkContext --- Conecction to the cluster
SparkSession --- Interface with that connection

# Using DataFrames
Spark's core data structure is the Resilient Distributed Dataset (RDD) 
DataFrames are more optimized for complicated operations than RDD's.
The Spark DataFrame is inmutable.

To start working with Spark DataFrames we have to create a SparkSession object
from our Spark Context.

Generally the SparkSesion is called spark.
SparkSession.builder.getOrCreate().

# Machine Learning Pipelines

Cleaning and preparing data for modelling

We need to import the pyspark.ml module because it has Transformer and Estimator
classes.
.transform(): It takes a DataFrame as an input and return a new DataFrame with one
column appended.
.fit(): It takes a DataFrame as an imput and return a model Object.

Strings and factors
Spark requires numeric data for modeling.
pyspark.ml.features
The first step to encoding our categorical feature is to create a StringIndexer.
The second step is to encode the last column by OneHotEncoder method.

Assemble Vector
The last step in pipeline is to combine all the columns containning our features into
a single column.
VectorAssembler()

Create Pipeline
This one wraps all columns up in a single object.

Test vs Train
We nned to split the data into a test set and train set.
randomSplit()

Select a model
pyspark.ml.regression ...
pyspark.ml.classification ...



