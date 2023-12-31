from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('Curso/Spark_DataFrames/people.json')
df.show()
df.printSchema()
df.columns
df.describe().show()

from pyspark.sql.types import (StructField, StringType,
                              IntegerType, StructType)

data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]

final_struc = StructType(fields = data_schema)
df = spark.read.json('Curso/Spark_DataFrames/people.json', schema = final_struc)
df.show()
df.printSchema()

df.select('age')
df.head(2)[0]
df.select(['age', 'name']).show()

df.withColumn('newage', df['age'] * 2).show() # crea la columna newage en una nueva columna
df.withColumnRenamed('age', 'new_age_2').show()

df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people WHERE age = 30")
results.show()

