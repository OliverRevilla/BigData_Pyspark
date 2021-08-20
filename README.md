# Spark_Pyspark
The first step to using Spark is connecting to a cluster.
To create one connection we should create an instance of the SparkContext (sc) class.
SparkConf()

SparkContext --- Connection to the cluster
SparkSession --- Interface with that connection

# Using RDD's or DataFrames

## RDD's 

.,Spark's core data structure is the Resilient Distributed Dataset (RDD)  
Resilient: Ability to withstand failures.
Distributed: Spanning across multiple machines.
Datasets: Collection of partitioned data.
sc.parallelize().
sc.textFile().

RDD transformations:
map()
filter()
flatMap()
union()

RDD actions:
collect(): returns all the elements of the dataset.
take(): returns an array with the certainly number of elements.
first(): prints the first element of the RDD. 
count(): returns the number of elements of the RDD.
reduce(): is used for aggregating the elements of a regular RDD.
saveAsTextFile(): saves an RDD into a text file with each partition as 
a separate file.
coalesce(): saves an RDD as a single text file.

Pair RDDS:
reduceByKey(): Combine values with the same key.
groupByKey(): Group values with the samen key.
sortByKey(): Return and RDD sorted by key.
join():Join two pair RDDs based on their key.
countByKey(): 
collectAsMap(): returns hte key-value pairs in the RDD as dictionary.

## DataFrames

DataFrames are more optimized for complicated operations than RDD's.
PySpark DataFrame is an inmutable distributed collection of data with named columns.
Designed for processing both structured and semi-structured data (JSON)
To start working with Spark DataFrames we have to create a SparkSession object
from our Spark Context.

Generally the SparkSesion is called spark.
SparkSession.builder.getOrCreate().
SparkSession is used to create DataFrame, register DataFrames and execute SQL queries.

Usual chunks:
spark.createDataFrame()
spark.read.csv()
spark.read.json()
spark.read.parquet()
spark.sql

DataFrame Transformations:
select()
filter()
groupby()
orderby()
dropDuplicates()
withColumn()
withColumnRenamed()


DataFrame Actions:
printSchema()
head()
show()
count()
describe()
.columns
filter()


## Executing SQL Queries
The SparkSession sql() method executes SQL Query.
df.createdOrReplaceTempView()
spark.sql('Expression')
## Visually Data
df.cov('col1','col2'), df.corr('col1','col2')
df.sample(False, 0.1,42).count()
df.sample(False, 0.2, 147).toPandas()
## Dropping data or missing values
df.drop(['col1','col2',...])
df.dropna(how = 'all', subset = ['col1','col2'...])
df.dropDuplicates(['col1])
## Assessing to missing values
df['col1'].isNull().count()
## Time features
from pyspark.sql.functions import to_date, year_month,dayofmonth,weekofyear,datediff,lag,window



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



