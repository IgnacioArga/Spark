from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("lrex").getOrCreate()

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction")

#### Conceptos
all_data = spark.read.format("libsvm").load("Curso/Spark_for_Machine_Learning/Linear_Regression/sample_linear_regression_data.txt")
all_data.show()

# Modelo con toda la data
lrModel = lr.fit(all_data)
lrModel.coefficients
lrModel.intercept

all_data_summary = lrModel.summary
all_data_summary.rootMeanSquaredError

# Separo data en training y testing
train_data, test_data = all_data.randomSplit([0.7,0.3])

correct_model = lr.fit(train_data)
test_results = correct_model.evaluate(test_data)

test_results.r2

# evaluo el modelo sobre datos sin labels (sin y)
unlabeled_data = test_data.select("features")
predictions = correct_model.transform(unlabeled_data)
predictions.show()

#### Ejemplo real
data = spark.read.csv("Curso/Spark_for_Machine_Learning/Linear_Regression/Ecommerce_Customers.csv", inferSchema= True, header=True)
data.printSchema()
data.show()

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

data.columns
map_rename = {col: col.replace(" ", "_") for col in data.columns}
data_renamed = data.select([data[col].alias(map_rename[col]) for col in data.columns])

data_renamed.columns
data.describe().show()
data_renamed.describe().show()

data_numerical = data_renamed[data_renamed.columns[3:-1]]

assembler = VectorAssembler(inputCols=data_numerical.columns, outputCol="features")
output = assembler.transform(data_renamed)

output.show()

final_data = output.select("features","Yearly_Amount_Spent")
final_data.head()

train_data,test_data = final_data.randomSplit([0.7,0.3])
lr = LinearRegression(labelCol="Yearly_Amount_Spent")
lr_model = lr.fit(train_data)
test_results = lr_model.evaluate(test_data)
test_results.r2
test_results.rootMeanSquaredError

# reeeeeplica de error cuadratico medio

unlabeled_data = test_data.select("features")

predictions = lr_model.transform(unlabeled_data)

prediction_bis = predictions.join(test_data, on="features", how="left")
error = prediction_bis.select(((prediction_bis.prediction - prediction_bis.Yearly_Amount_Spent)**2).alias("error")).agg({"error":"sum"})
error.select((error["sum(error)"]/predictions.count())**(0.5)).show() # raiz de error cuadratico medio