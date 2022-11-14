# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Store - 出租车数据集示例笔记本
# MAGIC 
# MAGIC 这个动手实验演示如何使用Feature Store创建一个预测纽约黄色出租车价格的模型。它包括以下步骤:
# MAGIC 
# MAGIC - 计算和写入特征
# MAGIC - 训练一个模型使用这些特征来预测票价。
# MAGIC - 在使用现有特征的新批数据上评估该模型，并保存到特征存储中。
# MAGIC 
# MAGIC ## 要求
# MAGIC - Databricks运行时用ML8.3或以上。 
# MAGIC 
# MAGIC **注意:** 本笔记本是为运行Databricks运行时ML 10.2或以上版本而编写的。如果您使用的是Databricks运行时ML10.1或以下版本，请删除或注释掉Cmd 19并取消注释Cmd 20。

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_flow_v3.png"/>

# COMMAND ----------

# MAGIC %md ## 计算特征

# COMMAND ----------

# MAGIC %md #### 加载用于计算特征的原始数据
# MAGIC 
# MAGIC 加载' nyc-taxi-tiny '数据集。这是由完整的[纽约市出租车数据](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)生成的，可以在' dbfs:/databricks-datasets/nyctaxi '中找到，通过应用以下转换:
# MAGIC 
# MAGIC 1. 应用UDF将经纬度坐标转换为邮政编码，并向DataFrame添加邮政编码列。
# MAGIC 1. 基于日期范围查询，使用Spark DataFrame API的.sample()方法将数据集再采样到一个更小的数据集。
# MAGIC 1. 重命名某些列并删除不必要的列。
# MAGIC 
# MAGIC 如果你想自己从原始数据创建这个数据集，请遵循以下步骤:
# MAGIC 1. 运行Feature Store出租示例数据集笔记本([AWS](https://docs.databricks.com/_static/notebooks/machine-learning/feature-store-taxi-example-dataset.html)|[Azure](https://docs.microsoft.com/azure/databricks/_static/notebooks/machine-learning/feature-store-taxi-example-dataset.html)|[GCP](https://docs.gcp.databricks.com/_static/notebooks/machine-learning/feature-store-taxi-example-dataset.html))去生成Delta table.
# MAGIC 1. 在这个notebook中，将下面的`spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")`替换为: `spark.read.table("feature_store_taxi_example.nyc_yellow_taxi_with_zips")`

# COMMAND ----------

raw_data = spark.read.format("delta").load("/databricks-datasets/nyctaxi-with-zipcodes/subsampled")
display(raw_data)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 从出租车价格交易数据中，我们将根据上车和下车的邮政编码计算两组特征。
# MAGIC 
# MAGIC #### Pickup features
# MAGIC 1. 行程次数 (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 1. 平均票价 (time window = 1 hour, sliding window = 15 minutes)
# MAGIC 
# MAGIC 
# MAGIC #### Drop off features
# MAGIC 1. 行程次数 (time window = 30 minutes)
# MAGIC 1. 旅行是否在周末结束(使用python代码的自定义功能)
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_computation_v5.png"/>

# COMMAND ----------

# MAGIC %md ### 辅助函数

# COMMAND ----------

from databricks import feature_store
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType
from pytz import timezone


@udf(returnType=IntegerType())
def is_weekend(dt):
    tz = "America/New_York"
    return int(dt.astimezone(timezone(tz)).weekday() >= 5)  # 5 = Saturday, 6 = Sunday
  
@udf(returnType=StringType())  
def partition_id(dt):
    # datetime -> "YYYY-MM"
    return f"{dt.year:04d}-{dt.month:02d}"


def filter_df_by_ts(df, ts_column, start_date, end_date):
    if ts_column and start_date:
        df = df.filter(col(ts_column) >= start_date)
    if ts_column and end_date:
        df = df.filter(col(ts_column) < end_date)
    return df


# COMMAND ----------

# MAGIC %md ### 数据科学家用于计算特征的自定义代码

# COMMAND ----------

def pickup_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the pickup_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df, ts_column, start_date, end_date
    )
    pickupzip_features = (
        df.groupBy(
            "pickup_zip", window("tpep_pickup_datetime", "1 hour", "15 minutes")
        )  # 1 hour window, sliding every 15 minutes
        .agg(
            mean("fare_amount").alias("mean_fare_window_1h_pickup_zip"),
            count("*").alias("count_trips_window_1h_pickup_zip"),
        )
        .select(
            col("pickup_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("mean_fare_window_1h_pickup_zip").cast(FloatType()),
            col("count_trips_window_1h_pickup_zip").cast(IntegerType()),
        )
    )
    return pickupzip_features
  
def dropoff_features_fn(df, ts_column, start_date, end_date):
    """
    Computes the dropoff_features feature group.
    To restrict features to a time range, pass in ts_column, start_date, and/or end_date as kwargs.
    """
    df = filter_df_by_ts(
        df,  ts_column, start_date, end_date
    )
    dropoffzip_features = (
        df.groupBy("dropoff_zip", window("tpep_dropoff_datetime", "30 minute"))
        .agg(count("*").alias("count_trips_window_30m_dropoff_zip"))
        .select(
            col("dropoff_zip").alias("zip"),
            unix_timestamp(col("window.end")).alias("ts").cast(IntegerType()),
            partition_id(to_timestamp(col("window.end"))).alias("yyyy_mm"),
            col("count_trips_window_30m_dropoff_zip").cast(IntegerType()),
            is_weekend(col("window.end")).alias("dropoff_is_weekend"),
        )
    )
    return dropoffzip_features  

# COMMAND ----------

from datetime import datetime

pickup_features = pickup_features_fn(
    raw_data, ts_column="tpep_pickup_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)
dropoff_features = dropoff_features_fn(
    raw_data, ts_column="tpep_dropoff_datetime", start_date=datetime(2016, 1, 1), end_date=datetime(2016, 1, 31)
)

# COMMAND ----------

display(pickup_features)

# COMMAND ----------

# MAGIC %md ### 使用特征存储库创建新的特征表

# COMMAND ----------

# MAGIC %md 首先，创建存储特征表的数据库。

# COMMAND ----------

# test_fs_db_100
dbutils.widgets.text("fs_db_name","")

# COMMAND ----------

fs_db_name = dbutils.widgets.get("fs_db_name")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE DATABASE IF NOT EXISTS $fs_db_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE test_fs_db_100

# COMMAND ----------

# MAGIC %md 接下来，创建一个特征存储客户端的实例。

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC 使用create_table API (Databricks运行时10.2 ML或更高)或create_feature_table API (Databricks运行时10.1 ML或更低)来定义模式和唯一的ID键。如果传递了可选参数df (Databricks运行时10.2 ML或更高)或features_df (Databricks运行时10.1 ML或更低)，API也会将数据写入特征存储。

# COMMAND ----------

# 该单元格使用Databricks运行时10.2 ML引入的API。
# 如果你的集群运行Databricks Runtime 10.1 ML或更低，跳过或注释掉这个单元格，取消注释并运行Cmd 22。
spark.conf.set("spark.sql.shuffle.partitions", "5")

fs.create_table(
    name=f"{fs_db_name}.trip_pickup_features",
    primary_keys=["zip", "ts"],
    df=pickup_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Pickup Features",
)
fs.create_table(
    name=f"{fs_db_name}.trip_dropoff_features",
    primary_keys=["zip", "ts"],
    df=dropoff_features,
    partition_columns="yyyy_mm",
    description="Taxi Fares. Dropoff Features",
)

# COMMAND ----------

# # 要在Databricks Runtime 10.1 ML或以下运行这个notebook，取消注释此单元格。

# spark.conf.set("spark.sql.shuffle.partitions", "5")

# fs.create_feature_table(
#     name=f"{fs_db_name}.trip_pickup_features",
#     keys=["zip", "ts"],
#     features_df=pickup_features,
#     partition_columns="yyyy_mm",
#     description="Taxi Fares. Pickup Features",
# )

# fs.create_feature_table(
#     name=f"{fs_db_name}.trip_dropoff_features",
#     keys=["zip", "ts"],
#     features_df=dropoff_features,
#     partition_columns="yyyy_mm",
#     description="Taxi Fares. Dropoff Features",
# )

# COMMAND ----------

# MAGIC %md ## 更新特征
# MAGIC 
# MAGIC 使用`write_table`函数更新特征表的值。
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_compute_and_write.png"/>

# COMMAND ----------

display(raw_data)

# COMMAND ----------

# 计算pickup_features特征组
pickup_features_df = pickup_features_fn(
  df=raw_data,
  ts_column="tpep_pickup_datetime",
  start_date=datetime(2016, 2, 1),
  end_date=datetime(2016, 2, 29),
)

# 将pickup features DataFrame写入特征存储表
fs.write_table(
  name=f"{fs_db_name}.trip_pickup_features",
  df=pickup_features_df,
  mode="merge",
)

# 计算dropoff_features特征组
dropoff_features_df = dropoff_features_fn(
  df=raw_data,
  ts_column="tpep_dropoff_datetime",
  start_date=datetime(2016, 2, 1),
  end_date=datetime(2016, 2, 29),
)

# 将dropoff features DataFrame写入特征存储表
fs.write_table(
  name=f"{fs_db_name}.trip_dropoff_features",
  df=dropoff_features_df,
  mode="merge",
)

# COMMAND ----------

# MAGIC %md 在写入时, 支持 `merge` 与 `overwrite`
# MAGIC 
# MAGIC     fs.write_table(
# MAGIC       name="feature_store_taxi_example.trip_pickup_features",
# MAGIC       df=pickup_features_df,
# MAGIC       mode="overwrite",
# MAGIC     )
# MAGIC     
# MAGIC 数据也可以通过传递一个dataframe来将其流式传输到特征存储中。`df.isStreaming`设置为True:
# MAGIC 
# MAGIC     fs.write_table(
# MAGIC       name="streaming_example.streaming_features",
# MAGIC       df=streaming_df,
# MAGIC       mode="merge",
# MAGIC     )
# MAGIC     
# MAGIC 你可以使用Databricks作业计划notebook定期更新功能 ([AWS](https://docs.databricks.com/jobs.html)|[Azure](https://docs.microsoft.com/azure/databricks/jobs)|[GCP](https://docs.gcp.databricks.com/jobs.html)).

# COMMAND ----------

# MAGIC %md 分析人员可以使用SQL与特征存储进行交互，例如:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(count_trips_window_30m_dropoff_zip) AS num_rides,
# MAGIC        dropoff_is_weekend
# MAGIC FROM   $fs_db_name.trip_dropoff_features
# MAGIC WHERE  dropoff_is_weekend IS NOT NULL
# MAGIC GROUP  BY dropoff_is_weekend;

# COMMAND ----------

# MAGIC %md ## 特征搜索和发现

# COMMAND ----------

# MAGIC %md
# MAGIC You can now discover your feature tables in the <a href="#feature-store/" target="_blank">Feature Store UI</a>.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/machine-learning/feature-store/taxi_example_flow_v3.png"/>
# MAGIC 
# MAGIC 通过“trip_pickup_features”或“trip_dropoff_features”搜索，可以查看表结构、元数据、数据源、提供者和在在线存储等详细信息。
# MAGIC 
# MAGIC 您还可以编辑特征表的描述，或者使用特征表名称旁边的下拉图标为特征表配置权限 
# MAGIC 
# MAGIC 查看[Use the Feature Store UI
# MAGIC ](https://docs.databricks.com/applications/machine-learning/feature-store.html#use-the-feature-store-ui) 文档获取更多细节.

# COMMAND ----------

# MAGIC %md ## T训练模型
# MAGIC 
# MAGIC 本节演示了如何使用存储在Feature Store中的pickup和dropoff特征来训练模型。它训练一个LightGBM模型来预测出租车价格。

# COMMAND ----------

# MAGIC %md ### 辅助函数

# COMMAND ----------

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

# MAGIC %md ### 读取出租车数据进行训练

# COMMAND ----------

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

# 如果你运行的是Databricks Runtime ML9.1或更低版本，你可以取消注释此单元格中的代码，使用它而不是Cmd 34中的代码。

from databricks.feature_store import FeatureLookup
import mlflow

pickup_features_table = f"{fs_db_name}.trip_pickup_features"
dropoff_features_table = f"{fs_db_name}.trip_dropoff_features"

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

# 结束任何现有的运行(在这个notebook第二次运行的情况下)
mlflow.end_run()

# 启动mlflow运行，这是特征存储记录模型所需的
mlflow.start_run() 

#因为舍入后的时间戳列可能会导致模型过拟合数据
#除非进行额外的特征工程，否则排除它们以避免对它们进行训练。
exclude_columns = ["rounded_pickup_datetime", "rounded_dropoff_datetime"]

# 创建训练集，将原始输入数据与两个特征表中对应的特征进行合并
training_set = fs.create_training_set(
  taxi_data,
  feature_lookups = pickup_feature_lookups + dropoff_feature_lookups,
  label = "fare_amount",
  exclude_columns = exclude_columns
)

# 将训练集加载到一个dataframe中，该dataframe可以传递给sklearn以训练模型
training_df = training_set.load_df()

# COMMAND ----------

#显示训练数据框，并注意它包含原始输入数据和来自特征存储的特征，如`dropoff_is_weekend`
display(training_df)

# COMMAND ----------

# MAGIC %md ## 下一步
# MAGIC 
# MAGIC 1. 在<a href="#feature-store">Feature Store UI</a>中探索本例中的特征表.
# MAGIC 1. 将该笔记本改编为您自己的数据并创建自己的特征表。