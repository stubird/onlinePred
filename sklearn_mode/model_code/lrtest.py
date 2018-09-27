import numpy as np
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib


# 加载鸢尾花数据
iris = load_iris()
# 创建包含特征名称的 DataFrame
df = pd.DataFrame(iris.data, columns=iris.feature_names)
df['species'] = pd.Categorical.from_codes(iris.target, iris.target_names)

# 生成标记，切分训练集、测试集
df['is_train'] = np.random.uniform(0, 1, len(df)) <= .75
train, test = df[df['is_train']==True], df[df['is_train']==False]

# 生成 X 和 y
features = df.columns[:4]
y = pd.factorize(train['species'])[0]

model = RandomForestClassifier(n_jobs=2)

# 训练模型
model.fit(train[features], y)
# 预测数据
model.predict(test[features])

# 保存模型到 model_code.joblib 文件
joblib.dump(model, "../model/model.joblib" ,compress=1)
