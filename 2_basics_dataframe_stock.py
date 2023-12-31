from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ops').getOrCreate()
df = spark.read.csv('Curso/Spark_DataFrames/appl_stock.csv', inferSchema=True, header=True)
df.show()

df.filter("Close < 200").select('Open').show() # SQL
df.filter(df['Close'] < 500).select('Open').show() # Python

df.filter((df['Close'] < 500) & (df['Close'] > 490)).select('Open').show() # Python

result = df.filter(df['Low'] == 197.16).collect()
result[0]
result[0].asDict()['Volume']