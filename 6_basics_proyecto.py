from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("walmart").getOrCreate()

df = spark.read.csv("Curso/Spark_DataFrame_Project_Exercise/walmart_stock.csv",header=True, inferSchema=True)
df.show()

#### What are the column names?
df.columns

#### What does the Schema look like?
df.printSchema()

#### Print out the first 5 rows.
for i in df.head(5):
    print(i)
    
#### Use describe() to learn about the DataFrame.
df.describe().show()

## Bonus Question!
#### There are too many decimal places for mean and stddev in the describe() dataframe. Format the numbers to just show up to two decimal places. Pay careful attention to the datatypes that .describe() returns, we didn't cover how to do this exact formatting, but we covered something very similar.
from pyspark.sql.functions import format_number,max,min,count,year,month
from pyspark.sql.types import (StructField, StringType,
                              IntegerType, StructType)

cols =[["summary"],[format_number(df2[i].cast("float"),2).alias(i) for i in df2.columns[1:]]]
lista = [item for sub_list in cols for item in sub_list]
df2.select(lista).show()

#### Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day.
df.select((df.High/df.Volume).alias("HV Ratio")).show()

#### What day had the Peak High in Price?

max_price = df.select(max("High")).collect()[0][0]

df.filter(df.High == max_price).select("Date").collect()[0][0]

#### What is the mean of the Close column?
df.select(mean("Close")).collect()[0][0]
df.select(mean("Close")).show()

#### What is the max and min of the Volume column?
df.select(max("Volume"), min("Volume")).show()

#### How many days was the Close lower than 60 dollars?
df.filter(df["Close"] < 60.0).select(count("Close")).collect()[0][0]

#### What percentage of the time was the High greater than 80 dollars ?
major_60 = df.filter(df["High"] > 80.0).select(count("High")).collect()[0][0]
total = df.select(count("High")).collect()[0][0]

major_60/total*100

#### What is the Pearson correlation between High and Volume?
df.stat.corr("High","Volume")

#### What is the max High per year?
df.withColumn("Year", year("Date")).groupBy("Year").agg(max("High")).show()

#### What is the average Close for each Calendar Month?
#### In other words, across all the years, what is the average Close price for Jan,Feb, Mar, etc... Your result will have a value for each of these months. 
df.withColumn("Month", month("Date")).groupBy("Month").agg(format_number(mean("Close"),2).alias("Avg Close")).orderBy("Month").show()


