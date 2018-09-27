import flask
from flask import Flask, request, url_for, Response
from sklearn.externals import joblib

app = Flask(__name__)

# 加载模型
model = joblib.load("../model/model.joblib")


@app.route("/", methods=["GET"])
def index():
    with app.test_request_context():
        # 生成每个函数监听的url以及该url的参数
        result = {"predict_iris": {"url": url_for("predict_iris"),
                                   "params": ["sepal_length", "sepal_width", "petal_length", "petal_width"]}}

        result_body = flask.json.dumps(result)

        return Response(result_body, mimetype="application/json")


@app.route("/ml/predict_iris", methods=["GET"])
def predict_iris():
    request_args = request.args

    # 如果没有传入参数，返回提示信息
    if not request_args:
        result = {
            "message": "请输入参数：sepal_length, sepal_width, petal_length, petal_width"
        }
        result_body = flask.json.dumps(result, ensure_ascii=False)
        return Response(result_body, mimetype="application/json")

    # 获取请求参数
    sepal_length = float(request_args.get("sepal_length", "-1"))
    sepal_width = float(request_args.get("sepal_width", "-1"))
    petal_length = float(request_args.get("petal_length", "-1"))
    petal_width = float(request_args.get("petal_width", -1))

    # 构建特征矩阵
    vec = [[sepal_length, sepal_width, petal_length, petal_width]]
    print("vec: {0}".format(vec))

    # 生成预测结果
    predict_result = int(model.predict(vec)[0])
    print("predict_result: {0}".format(predict_result))

    # 构造返回数据
    result = {
        "features": {
            "sepal_length": sepal_length,
            "sepal_width": sepal_width,
            "petal_length": petal_length,
            "petal_width": petal_width
        },
        "result": predict_result
    }

    result_body = flask.json.dumps(result, ensure_ascii=False)
    return Response(result_body, mimetype="application/json")


if __name__ == "__main__":
    app.run(port=8000)
    #http: // 127.0.0.1: 8000 / ml / predict_iris?sepal_length = 10 & sepal_width = 1 & petal_length = 3 & petal_width = 2