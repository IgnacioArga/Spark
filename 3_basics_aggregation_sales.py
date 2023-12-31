from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("aggs").getOrCreate()

df = spark.read.csv('Curso/Spark_DataFrames/sales_info.csv', inferSchema= True, header = True)
df.show()

df.groupBy("Company").mean().show()

df.groupBy("Company").sum().show()

df.agg({"Sales": "sum"}).show()
df.groupBy("Company").agg({"Sales": "sum", "Person":"count"}).show()

from pyspark.sql.functions import countDistinct,avg,stddev,format_number

df.select(countDistinct("Sales")).show()
df.select(avg("Sales")).alias("Average Sales").show()

df.groupBy("Company").agg(
    format_number(stddev("Sales"),2).alias("std"),
    avg("Sales").alias("avg")
    ).show()

df.groupBy("Company").agg(
    format_number(stddev("Sales"),2).alias("std"),
    avg("Sales").alias("avg")
    ).orderBy("avg").show()