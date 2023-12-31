#### 0 - Objective
# give accurate estimates of how many crew members a ship will require

#### 1 - Import libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.ml.regression import LinearRegression
import numpy as np
import pandas as pd

#### 2 - Create Spark Session
spark = SparkSession.builder.appName("cruise").getOrCreate()

#### 3 - Import data
data = spark.read.csv("Linear_Regression_project/cruise_ship_info.csv",inferSchema=True,header=True)

#### 4 - Exploratory Analysis
data.printSchema()

def na_count(data):
    na = {col: data.filter(f"{col} IS NULL").count() for col in data.columns}
    return na
na_count(data) 

data.describe().show()

#### 5 - Data Wrangling
# Cruise_line to dummie
def get_spark_dummy(data, cols):
    df = data.select(cols)
    unique_values = {col:df.select(col).distinct().collect() for col in cols} 
    values = {col:[unique_values[col][i][0] for i in range (len(unique_values[col])-1) for col in unique_values] for col in unique_values}
    for col in values:
        col_val = np.identity(len(values[col]))
        dummy_table = pd.DataFrame(data=col_val,columns=values[col])
        dummy_table[col] = values[col]
        dummy_table = spark.createDataFrame(dummy_table.iloc[:,1:])
        data = data.join(dummy_table,on=col).drop(col)
        
    return data
    
cols = ["Cruise_line"]
data_dummy = get_spark_dummy(data,cols)
data_dummy.columns

#### 6 - Model

data_test, data_train = data_dummy.randomSplit([0.7,0.3])




