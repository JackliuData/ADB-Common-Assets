# Databricks notebook source
# MAGIC %md
# MAGIC #### 为train_model notebook准备
# MAGIC 
# MAGIC 假设:你已经在特征工程步骤中创建了所有需要的特征。这个笔记本将形成train_df并开始为你的模型训练过程进行mlflow跟踪。我们假设用户在开发环境中工作时，允许将训练好的模型注册到模型暂存库中。再次触发

# COMMAND ----------

dbutils.widgets.dropdown("register_to_staging", "True", ["True", "False"], "Register model to staging")
dbutils.widgets.text("raw_data_path", "/databricks-datasets/nyctaxi-with-zipcodes/subsampled", "Raw path for data source")
# you should also parameterize the usage of feature tables etc, for simplicity we skip those
# get var to decide if register to staging registry
register_staging = dbutils.widgets.get("register_to_staging")
raw_data_path = dbutils.widgets.get("raw_data_path")

# COMMAND ----------

# DBTITLE 1,辅助函数准备训练
from pyspark.sql import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import IntegerType
import math
from datetime import timedelta
import mlflow.pyfunc

def rounded_unix_timestamp(dt, num_minutes=15):
    """
    Ceilings datetime dt to interval num_minutes, then returns the unix timestamp.
    """
    nsecs = dt.minute * 60 + dt.second + dt.microsecond * 1e-6
    delta = math.ceil(nsecs / (60 * num_minutes)) * (60 * num_minutes) - nsecs
    return int((dt + timedelta(seconds=delta)).timestamp())

rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())

def rounded_taxi_data(taxi_data_df):
    # Round the taxi data timestamp to 15 and 30 minute intervals so we can join with the pickup and dropoff features
    # respectively.
    taxi_data_df = (
        taxi_data_df.withColumn(
            "rounded_pickup_datetime",
            rounded_unix_timestamp_udf(taxi_data_df["tpep_pickup_datetime"], lit(15)),
        )
        .withColumn(
            "rounded_dropoff_datetime",
            rounded_unix_timestamp_udf(taxi_data_df["tpep_dropoff_datetime"], lit(30)),
        )
        .drop("tpep_pickup_datetime")
        .drop("tpep_dropoff_datetime")
    )
    taxi_data_df.createOrReplaceTempView("taxi_data")
    return taxi_data_df
  
def get_latest_model_version(model_name):
    latest_version = 1
    mlflow_client = MlflowClient()
    for mv in mlflow_client.search_model_versions(f"name='{model_name}'"):
        version_int = int(mv.version)
        if version_int > latest_version:
            latest_version = version_int
    return latest_version

# COMMAND ----------

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone

raw_data = spark.read.format("delta").load(raw_data_path)
taxi_data = rounded_taxi_data(raw_data)

# COMMAND ----------

# MAGIC %md ### 理解训练数据集是如何创建的
# MAGIC 
# MAGIC 为了训练模型，你需要创建用于训练模型的训练数据集。训练数据集由以下部分组成:
# MAGIC 
# MAGIC 1. 原始输入数据
# MAGIC 1. 特征存储中的特征
# MAGIC 
# MAGIC 原始输入数据包含:
# MAGIC 
# MAGIC 1. 用于连接特征的主键。
# MAGIC 1. 像“trip_distance”这样的原始特征不在特征库中。
# MAGIC 1. 模型训练所需的预测目标，如“fare”。
# MAGIC 
# MAGIC 下面是一个可视化的概述，展示了原始输入数据与Feature Store中的特征相结合，以生成训练数据集:
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_feature_lookup.png"/>
# MAGIC 
# MAGIC 这些概念将在创建训练数据集文档中进一步描述 ([AWS](https://docs.databricks.com/applications/machine-learning/feature-store.html#create-a-training-dataset)|[Azure](https://docs.microsoft.com/en-us/azure/databricks/applications/machine-learning/feature-store#create-a-training-dataset)|[GCP](https://docs.gcp.databricks.com/applications/machine-learning/feature-store.html#create-a-training-dataset)).
# MAGIC 
# MAGIC 下一个单元格通过为每个需要的特征创建`FeatureLookup`从特征存储中加载特征以进行模型训练。

# COMMAND ----------

# If you are running Databricks Runtime for Machine Learning 9.1 or above, you can uncomment the code in this cell and use it instead of the code in Cmd 34.

from databricks.feature_store import FeatureLookup
import mlflow

pickup_features_table = "feature_store_taxi_example.trip_pickup_features"
dropoff_features_table = "feature_store_taxi_example.trip_dropoff_features"

pickup_feature_lookups = [
    FeatureLookup( 
      table_name = pickup_features_table,
      feature_names = ["mean_fare_window_1h_pickup_zip", "count_trips_window_1h_pickup_zip"],
      lookup_key = ["pickup_zip", "rounded_pickup_datetime"],
    ),
]

dropoff_feature_lookups = [
    FeatureLookup( 
      table_name = dropoff_features_table,
      feature_names = ["count_trips_window_30m_dropoff_zip", "dropoff_is_weekend"],
      lookup_key = ["dropoff_zip", "rounded_dropoff_datetime"],
    ),
]

# COMMAND ----------

# MAGIC %md ### 创建训练数据集
# MAGIC 
# MAGIC 当下面调用`fs.create_training_set(..)`时，将发生以下步骤:
# MAGIC 
# MAGIC 1. 将创建一个`TrainingSet`对象，它将从特征存储中选择用于训练模型的特定特征。每个特性都由上面创建的`FeatureLookup`指定。
# MAGIC 
# MAGIC 1. 根据每个`FeatureLookup`的`lookup_key`将特征与原始输入数据连接起来。
# MAGIC 
# MAGIC 然后将`TrainingSet`转换为用于训练的DataFrame。这个Datafeame包括taxi_data的列，以及在` featurelookup `中指定的特征。

# COMMAND ----------

# MAGIC %md
# MAGIC 根据“TrainingSet.to_df”返回的数据训练一个LightGBM模型，然后使用“FeatureStoreClient.log_model”记录模型。模型将与特征元数据打包在一起。

# COMMAND ----------

from sklearn.model_selection import train_test_split
from mlflow.tracking import MlflowClient
import lightgbm as lgb
import mlflow.lightgbm
from mlflow.models.signature import infer_signature

# COMMAND ----------

import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn import metrics
import numpy as np

def train_model(taxi_data, pickup_feature_lookups, dropoff_feature_lookups):
    # 启动mlflow运行，这是特征存储记录模型所需要的
    # 结束任何活动的运行
    mlflow.end_run()
    mlflow.set_experiment("/Users/zengyx@shnywb.onmicrosoft.com/ML/Demos/train_model")
    
    with mlflow.start_run(run_name="Basic RF Experiment") as run:
        exclude_columns = ["rounded_pickup_datetime", "rounded_dropoff_datetime"]
        fs = feature_store.FeatureStoreClient()
        # 创建训练集，其中包括原始输入数据与来自两个特征表的相应特征
        training_set = fs.create_training_set(
          taxi_data,
          feature_lookups = pickup_feature_lookups + dropoff_feature_lookups,
          label = "fare_amount",
          exclude_columns = exclude_columns
        )

        # 将训练集加载到一个Dataframe中，该Dataframe可以传递到sklearn中用于训练模型
        training_df = training_set.load_df()
        features_and_label = training_df.columns
        # 将数据收集到Pandas数组中进行训练
        data = training_df.toPandas()[features_and_label]
        
        # force dropna -> in reality do proper processing work!
        data_dropna = data.dropna().reset_index().drop("index", axis=1)
        
        # pandas df input
        train, test = train_test_split(data_dropna, test_size=0.2, random_state=0)
        X_train = train.drop(["fare_amount"], axis=1)
        X_test = test.drop(["fare_amount"], axis=1)

        sc = StandardScaler()
        X_train = sc.fit_transform(X_train)
        X_test = sc.transform(X_test)
        
        y_train = train.fare_amount
        y_test = test.fare_amount

        regressor = RandomForestRegressor(n_estimators=20, random_state=0)
        regressor.fit(X_train, y_train)
        y_pred = regressor.predict(X_test)

        print('Mean Absolute Error:', metrics.mean_absolute_error(y_test, y_pred))
        print('Mean Squared Error:', metrics.mean_squared_error(y_test, y_pred))
        print('Root Mean Squared Error:', np.sqrt(metrics.mean_squared_error(y_test, y_pred)))

        mse = metrics.mean_squared_error(y_test, y_pred)
        # Log model
        mlflow.sklearn.log_model(regressor, "random-forest-model")
        # Log metrics
        mlflow.log_metric("mse", mse)

        runID = run.info.run_id
        experimentID = run.info.experiment_id

        print(f"Inside MLflow Run with run_id `{runID}` and experiment_id `{experimentID}`")

        if register_staging == "True":
            # 使用MLflow记录训练过的模型，并将其与特征查找信息打包。
            fs.log_model(
              regressor,
              artifact_path="model_packaged",
              flavor=mlflow.sklearn,
              training_set=training_set,
              registered_model_name="taxi_example_fare_packaged"
            )
    return regressor

# COMMAND ----------

model = train_model(taxi_data, pickup_feature_lookups, dropoff_feature_lookups)

# COMMAND ----------

