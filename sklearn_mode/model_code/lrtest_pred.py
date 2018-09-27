from sklearn.externals import joblib

# 加载模型文件，生成模型对象
new_model = joblib.load("../model/model.joblib")

new_pred_data = [[0.1, 0.3, 0.7, 0.1]]
# 使用加载生成的模型预测新样本
print(new_model.predict(new_pred_data))