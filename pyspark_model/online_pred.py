import flask
from flask import Flask, request, url_for, Response
from sklearn.externals import joblib
from pyspark.mllib.tree import GradientBoostedTreesModel
from pyspark.mllib.linalg import SparseVector
from pyspark import SparkContext,SparkConf
import json
app = Flask(__name__)

# 加载模型
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

model =GradientBoostedTreesModel.load(sc,'./sellModel')

@app.route("/", methods=["GET"])
def index():
    with app.test_request_context():
        # 生成每个函数监听的url以及该url的参数
        result = {"gbdt": {"url": url_for("gbdt"),
                                   "params": ["vector"]}}

        result_body = flask.json.dumps(result)

        return Response(result_body, mimetype="application/json")

@app.route("/ml/gbdt", methods=["GET"])
def gbdt():
    request_args = request.args

    # 如果没有传入参数，返回提示信息
    if not request_args:
        result = {
            "message": "请输入参数：vector"
        }
        result_body = flask.json.dumps(result, ensure_ascii=False)
        return Response(result_body, mimetype="application/json")

    # 获取请求参数
    vector = request_args.get("vector", "{vector=[[0,0,0,0,0,0,0]]}")
    print(vector)
    jsob = json.loads(vector)
    vector = [[float(j) for j in i] for i in jsob["vector"]]
    ret = [model.predict(arr) for arr in vector]
    print("predict result :" + str(ret))

    # 构造返回数据
    result = {
        "features": {
            "vector":vector
        },
        "result": ret
    }

    result_body = flask.json.dumps(result, ensure_ascii=False)
    return Response(result_body, mimetype="application/json")


if __name__ == "__main__":
    app.run(port=8000)
    #http://127.0.0.1:8000/ml/gbdt?sepal_length=10&sepal_width=1&petal_length=3&petal_width=2