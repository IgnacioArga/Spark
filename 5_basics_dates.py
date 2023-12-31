from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dates").getOrCreate()

df = spark.read.csv('Curso/Spark_DataFrames/appl_stock.csv', inferSchema= True, header = True)

from pyspark.sql.functions import (dayofmonth,hour,dayofyear,
                                   month,year,weekofyear,
                                   format_number,date_format)

df.select("Date", dayofmonth(df["Date"])).show()

df.select("*",year(df["Date"]).alias("Year"),month(df["Date"]).alias("Month")).show()

df.select("*",year(df["Date"]).alias("Year")).groupBy("Year").agg(format_number(mean("Close"),2).alias("AvgClose")).show()