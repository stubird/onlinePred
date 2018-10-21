from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTreesModel
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext,SparkConf
from pyspark.ml.util import MLReader
from random import random

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#loader = MLReader()
#model = loader.load('./model')
model = GradientBoostedTreesModel.load(sc,'./gbdymodelonlionev1')

rdd = sc.parallelize([[i for i in range(16)], [i*2000 for i in range(16)],[random() for i in range(16)]])
print(model.predict(rdd).collect())

#model.predct(sparkpdfrompandas.rdd.map(lambda x:list(x)))
print(model)

#print(model.predict(SparseVector(2, {0: 1.0})))
