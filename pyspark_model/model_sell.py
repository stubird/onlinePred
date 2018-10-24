from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import MapType,IntegerType,StringType,DataType
from pyspark.sql.types import StructType,StructField
from pyspark.ml.linalg import Matrices
from pyspark.mllib.util import MLUtils
import pandas as pd

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
sqlc = SQLContext(sc)

print(StructField , "go go go")

#df = sqlc.read.csv("hdfs://hadoop1:9000/home/hadoop/test.csv",header=True)

sel = pd.read_csv("selldata/selldata")

df = sqlc.read.csv("selldata/selldata",header=True)
print(sel.columns)
print(df)

tran = df.rdd.map(lambda x:LabeledPoint(list(x)[9], SparseVector(7, [i for i in range(7)],list(x)[2:9])))

print(len(tran.collect()))


#tran = data.rdd.map(lambda x:LabeledPoint(list(x)[17], SparseVector(16, [i for i in range(16)],list(x)[1:17])))

#model = GradientBoostedTrees.trainRegressor(tran, {}, numIterations=10)

#model.save(sc,"./gbdymodelonlionev1")
#
# a = StructType([
#     StructField("ID",StringType(),False),
#     StructField("cust_type", StringType(), True),
#     StructField("cust_level", IntegerType(), True),
#     StructField("ID",StringType(),False),
#     StructField("cust_type", StringType(), True),
#     StructField("cust_level", IntegerType(), True),
#     StructField("cust_level", IntegerType(), True)
# ])

# print(a)