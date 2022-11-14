# Databricks notebook source
# MAGIC %md ### 训练模型并添加到mlflow注册表

# COMMAND ----------

dbutils.widgets.text(name = "model_name", defaultValue = "simple-model-100", label = "Model Name")

# COMMAND ----------

model_name=dbutils.widgets.get("model_name")

# COMMAND ----------

# MAGIC %md ### 连接到MLflow跟踪服务器
# MAGIC 
# MAGIC MLflow可以收集关于模型训练会话的数据，例如验证精度。它还可以保存训练期间生成的组件，例如PySpark流水线模型。
# MAGIC 
# MAGIC 默认情况下，这些数据和工件存储在集群的本地文件系统上。然而，它们也可以使用[MLflow跟踪服务器](https://mlflow.org/docs/latest/tracking.html)远程存储。

# COMMAND ----------

import mlflow
mlflow.__version__

# 使用托管的mlflow跟踪服务器

# COMMAND ----------

# MAGIC %md ## 训练模型

# COMMAND ----------

# MAGIC %md ### 下载训练数据
# MAGIC 
# MAGIC 首先，下载用于训练模型的[葡萄酒质量数据集 (published by Cortez et al.)](https://archive.ics.uci.edu/ml/datasets/wine+quality)。

# COMMAND ----------

#%sh wget https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv

# COMMAND ----------

#%sh cp ./winequality-red.csv /dbfs/FileStore/tables/winequality_red.csv

# COMMAND ----------

wine_data_path = "/dbfs/FileStore/tables/winequality_red.csv"

# COMMAND ----------

import pandas as pd
pdf = pd.read_csv(wine_data_path, sep=";")
pdf.head()

# COMMAND ----------

# MAGIC %md ### 在MLflow中运行、训练并保存ElasticNet模型，以对葡萄酒进行评级
# MAGIC 
# MAGIC 我们将使用Scikit-learn的Elastic Net回归模块训练一个模型。我们将在新的MLflow运行(训练会话)中拟合模型，允许我们保存特征指标、超参数数据和模型工件以供将来参考。如果MLflow已连接到跟踪服务器，此数据将持久化到跟踪服务器的文件和工件存储，允许其他用户查看和下载它。有关MLflow中模型跟踪的更多信息，请参阅[MLflow tracking reference](https://www.mlflow.org/docs/latest/tracking.html)。

# COMMAND ----------

import os
import warnings
import sys

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


input_example = {
  "fixed acidity": 7.4,
  "volatile acidity": 0.70,
  "citric acid": 1.4,
  "residual sugar": 0.2,
  "chlorides": 7.4,
  "free sulfur dioxide": 0.70,
  "total sulfur dioxide": 1.4,
  "density": 0.2,
  "pH": 7.4,
  "sulphates": 0.70,
  "alcohol": 1.4,
}
  
def train_model(wine_data_path, model_path, alpha, l1_ratio):
  warnings.filterwarnings("ignore")
  np.random.seed(40)

  # 读取葡萄酒质量的csv文件(确保你是从MLflow的根目录运行它!)
  data = pd.read_csv(wine_data_path, sep=None)

  # 将数据划分为训练集和测试集。(0.75, 0.25)拆分。
  train, test = train_test_split(data)

  # 预测的列是“quality”，它是一个从[3,9]开始的标量。
  train_x = train.drop(["quality"], axis=1)
  test_x = test.drop(["quality"], axis=1)
  train_y = train[["quality"]]
  test_y = test[["quality"]]

  # 开始一个新的MLflow训练运行 
  with mlflow.start_run():
    # 拟合Scikit-learn的ElasticNet模型
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
    lr.fit(train_x, train_y)

    predicted_qualities = lr.predict(test_x)

    # 使用几个精度指标评估模型的性能
    (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

    print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
    print("  RMSE: %s" % rmse)
    print("  MAE: %s" % mae)
    print("  R2: %s" % r2)

    # 将模型超参数和性能指标记录到MLflow跟踪服务器
    # (or to disk if no)
    mlflow.log_param("alpha", alpha)
    mlflow.log_param("l1_ratio", l1_ratio)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)

    mlflow.sklearn.log_model(lr, model_path, input_example=input_example)
    return mlflow.active_run().info.run_uuid

# COMMAND ----------

alpha_1 = 0.75
l1_ratio_1 = 0.25
model_path = 'model'
run_id1 = train_model(wine_data_path=wine_data_path, model_path=model_path, alpha=alpha_1, l1_ratio=l1_ratio_1)
model_uri = "runs:/"+run_id1+"/model"

# COMMAND ----------

print(model_uri)

# COMMAND ----------

# MAGIC %md ## 在模型注册表中注册模型

# COMMAND ----------

import time
result = mlflow.register_model(
    model_uri,
    model_name
)
time.sleep(10)
version = result.version

# COMMAND ----------

# MAGIC %md ### 将模型转换为` Staging `

# COMMAND ----------

import mlflow
client = mlflow.tracking.MlflowClient()

client.transition_model_version_stage(
    name=model_name,
    version=version,
    stage="staging")

# COMMAND ----------

