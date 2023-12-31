from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("miss").getOrCreate()

df = spark.read.csv('Curso/Spark_DataFrames/ContainsNull.csv', inferSchema= True, header = True)

df.show()
df.na.drop().show()
df.na.drop(thresh=2).show()
df.na.drop(how="any").show()
df.na.drop(how="all").show()
df.na.drop(subset=["sales"]).show()

df.printSchema()
df.na.fill("FILL VALUE").show()
df.na.fill(0).show()

from pyspark.sql.functions import mean

mean_sales = df.select(mean(df["Sales"])).collect()
df.na.fill({"Name": "anonimus","Sales":mean_sales[0][0]}).show()

