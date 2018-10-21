from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext,SparkConf

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

sparse_data = [
LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
]

data = sc.parallelize(sparse_data)

model = GradientBoostedTrees.trainRegressor(data, {}, numIterations=10)
model.numTrees()

model.totalNumNodes()

model.predict(SparseVector(2, {1: 1.0}))

model.predict(SparseVector(2, {0: 1.0}))

rdd = sc.parallelize([[0.0, 1.0], [1.0, 0.0]])
print(model.predict(rdd).collect())

model.save(sc,'model')