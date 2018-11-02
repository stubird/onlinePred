import numpy as np
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)
sqld = SQLContext(sc)

product_cate = ['current','regular','financing','fund']
current = ['int1','int2','int3','int4']
regular = ['reg1','reg2','reg3','reg4']
financing = ['fin1','fin2','fin3','fin4']
fun = ['fun1','fun2','fun3','fun4']

tran_table_schema = ['tran_id','tran_date','user_id','pro_id']

def func1():
    with open('TRANS.csv') as fp:
        with open('tiny_trans.csv','w') as tp:
            k = 0
            for i in fp.readlines():
                if k < 1000:
                    k+= 1
                else:
                    break
                tp.write(i)

def create_tran_table():
    table = sqld.read.csv('tiny_trans.csv',header=True)
    table.createTempView("trans")

    with open('./createFeature.sql') as fp:
        sqlstr = fp.read()
    #sqlstr = "select *, last(IF(pro_id >= 9 and pro_id <= 12, amt, null)) over(partition by user_id order by unix_timestamp(TRAN_DATE,'yyyy-MM-dd') range between  31968000 preceding and 604800 preceding) as sumamt from trans"
    print(sqlstr)
    data = sqld.sql(sqlstr)
    data.show()

    data.filter(lambda x:print(type(x)))
    data.orderBy(["user_id","TRAN_DATE"]).show()
create_tran_table()