# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 青铜层 到 银层 - ETL into a Silver table
# MAGIC 
# MAGIC 我们需要对数据进行一些转换，将其从青铜表移动到银表。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 笔记本目标
# MAGIC 
# MAGIC 在这个笔记本中:
# MAGIC 
# MAGIC 1. 使用可组合函数采集原始数据
# MAGIC 1. 使用可组合的函数来写入青铜表
# MAGIC 1. 发展青铜到银的步骤
# MAGIC   - 提取原始字符串并将其转换为列
# MAGIC   - 隔离不良数据
# MAGIC   - 将干净的数据加载到Silver表中
# MAGIC 1. 更新Bronze表中的记录状态

# COMMAND ----------

# MAGIC %md
# MAGIC ## 配置

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 导入操作函数

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### 显示青铜路径中的文件

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 获取更多原始数据
# MAGIC 
# MAGIC 在我们开始这个实验室之前，让我们获得更多的原始数据。
# MAGIC 
# MAGIC 在生产环境中，我们可能每小时都有数据传入。在这里，我们使用函数ingest_classic_data来模拟这一点。
# MAGIC 
# MAGIC 😎 回想一下，我们在笔记本00_ingest_raw中做过这个操作。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **练习:**使用功能函数加载10个小时。, `ingest_classic_data`.

# COMMAND ----------

# ANSWER
ingest_classic_data(hours=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta当前架构
# MAGIC 接下来，我们在Delta架构中演示到目前为止构建的所有内容。
# MAGIC 
# MAGIC 我们不像以前那样使用临时查询来实现，而是使用包含在文件`classic/includes/main/python/operations`中的可组合函数。你应该检查这个文件，以便在接下来的三步中使用正确的参数。
# MAGIC 
# MAGIC 🤔 如果你遇到困难，可以参考`02_bronze_to_silver`。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ###第一步:创建`rawDF`DataFrame
# MAGIC 
# MAGIC **练习:**使用`read_batch_raw`函数获取新的数据。

# COMMAND ----------

# ANSWER
rawDF = read_batch_raw(spark, rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步骤2:转换原始数据
# MAGIC 
# MAGIC **练习:**使用`transform_raw`函数采集新的数据。

# COMMAND ----------

# ANSWER
transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用断言验证模式
# MAGIC 
# MAGIC `transformedRawDF`dataframe现在应该具有以下模式:
# MAGIC 
# MAGIC ```
# MAGIC datasource: string
# MAGIC ingesttime: timestamp
# MAGIC value: string
# MAGIC status: string
# MAGIC p_ingestdate: date
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import *

assert transformedRawDF.schema == StructType(
    [
        StructField("datasource", StringType(), False),
        StructField("ingesttime", TimestampType(), False),
        StructField("status", StringType(), False),
        StructField("value", StringType(), True),
        StructField("p_ingestdate", DateType(), False),
    ]
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 步骤2:批写入青铜表
# MAGIC 
# MAGIC **练习:** 使用`batch_writer`函数来采集新的数据。
# MAGIC 
# MAGIC **注意**:你需要在写入器上使用`.save()`方法开始写入。
# MAGIC 
# MAGIC 🤖 **请确保分区在`p_ingestdate`上**.

# COMMAND ----------

# ANSWER
rawToBronzeWriter = batch_writer(dataframe=transformedRawDF, partition_column="p_ingestdate")

rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 清除原始文件路径
# MAGIC 
# MAGIC 手动清除已经加载的原始文件。

# COMMAND ----------

dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 显示青铜表
# MAGIC 
# MAGIC 如果你摄入了16个小时，你应该看到160条记录。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM health_tracker_classic_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## 青铜到银
# MAGIC 
# MAGIC 让我们开始青铜到银的步骤。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使笔记本保持一致

# COMMAND ----------

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records从青铜记录中加载新的记录
# MAGIC 
# MAGIC **练习**
# MAGIC 
# MAGIC 从状态为`"new"`的青铜表表中加载所有记录。

# COMMAND ----------

# ANSWER

bronzeDF = spark.read.table("health_tracker_classic_bronze").filter("status = 'new'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 从青铜记录中提取嵌套的JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: 从`value`列中提取嵌套的JSON
# MAGIC **练习**
# MAGIC 
# MAGIC 使用`pyspark.sql`函数将`value`列提取为新列`nested_json`。

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import from_json

json_schema = """
    time TIMESTAMP,
    name STRING,
    device_id STRING,
    steps INTEGER,
    day INTEGER,
    month INTEGER,
    hour INTEGER
"""

bronzeAugmentedDF = bronzeDF.withColumn("nested_json", from_json(col("value"), json_schema))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: 通过拆解`nested_json`列来创建Silver DataFrame
# MAGIC 
# MAGIC 解包JSON列意味着扁平化JSON，将每个顶级属性作为自己的列。
# MAGIC 
# MAGIC 🚨 **重要内容**请确保在Silver DataFrame中包含`"value"`列，因为稍后我们将使用它作为青铜表中每个记录的唯一引用

# COMMAND ----------

# ANSWER
silver_health_tracker = bronzeAugmentedDF.select("value", "nested_json.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用断言验证模式
# MAGIC 
# MAGIC DataFrame `silver_health_tracker`现在应该具有以下结构:
# MAGIC 
# MAGIC ```
# MAGIC value: string
# MAGIC time: timestamp
# MAGIC name: string
# MAGIC device_id: string
# MAGIC steps: integer
# MAGIC day: integer
# MAGIC month: integer
# MAGIC hour: integer
# MAGIC ```
# MAGIC 
# MAGIC 💪🏼 记住，`_parse_datatype_string`函数将DDL格式的模式字符串转换为Spark模式。

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert silver_health_tracker.schema == _parse_datatype_string(
    """
  value STRING,
  time TIMESTAMP,
  name STRING,
  device_id STRING,
  steps INTEGER,
  day INTEGER,
  month INTEGER,
  hour INTEGER
"""
)
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 转换数据
# MAGIC 
# MAGIC 1. 从列`time`中创建一列`p_eventdate DATE`。
# MAGIC 1. 将列`time`重命名为`eventtime`。
# MAGIC 1. 将`device_id`转换为整数。
# MAGIC 1. 按此顺序只包含以下列:
# MAGIC    1. `value`
# MAGIC    1. `device_id`
# MAGIC    1. `steps`
# MAGIC    1. `eventtime`
# MAGIC    1. `name`
# MAGIC    1. `p_eventdate`
# MAGIC 
# MAGIC 💪🏼 请记住，我们将新列命名为`p_eventdate`，以表明我们对这一列进行了分区。
# MAGIC 
# MAGIC 🕵🏽‍♀️ 请记住，我们将`value`作为青铜表中值的唯一引用。

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

silver_health_tracker = silver_health_tracker.select(
    "value",
    col("device_id").cast("integer").alias("device_id"),
    "steps",
    col("time").alias("eventtime"),
    "name",
    col("time").cast("date").alias("p_eventdate"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用断言验证模式
# MAGIC 
# MAGIC DataFrame `silver_health_tracker_data_df`现在应该具有以下模式:
# MAGIC 
# MAGIC ```
# MAGIC value: string
# MAGIC device_id: integer
# MAGIC heartrate: double
# MAGIC eventtime: timestamp
# MAGIC name: string
# MAGIC p_eventdate: date```
# MAGIC 
# MAGIC 💪🏼 记住，`_parse_datatype_string`函数将DDL格式的模式字符串转换为Spark模式。

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string

assert silver_health_tracker.schema == _parse_datatype_string(
    """
  value STRING,
  device_id INTEGER,
  steps INTEGER,
  eventtime TIMESTAMP,
  name STRING,
  p_eventdate DATE
"""
), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 隔离异常数据
# MAGIC 
# MAGIC 回想一下，在`00_ingest_raw`步骤中，我们发现一些记录以uuid字符串而不是字符串编码的整数形式传入device_id。我们的银表将device_ids存储为整数，因此很明显传入的数据存在问题。
# MAGIC 
# MAGIC 为了正确处理此数据质量问题，我们将隔离不良记录以供后续处理。

# COMMAND ----------

# MAGIC %md
# MAGIC 检查是否有null记录——比较下面两个单元格的输出

# COMMAND ----------

silver_health_tracker.count()

# COMMAND ----------

silver_health_tracker.na.drop().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 分割银级别DataFrame

# COMMAND ----------

silver_health_tracker_clean = silver_health_tracker.filter("device_id IS NOT NULL")
silver_health_tracker_quarantine = silver_health_tracker.filter("device_id IS NULL")

# COMMAND ----------

silver_health_tracker_quarantine.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 显示隔离记录

# COMMAND ----------

display(silver_health_tracker_quarantine)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 将干净的数据批量写入银级别表
# MAGIC 
# MAGIC **练习:**批量写入`silver_health_tracker_clean`到银表路径`silverPath`中。
# MAGIC 
# MAGIC 1. Use format, `"delta"`
# MAGIC 1. Use mode `"append"`.
# MAGIC 1. Do **NOT** include the `value` column.
# MAGIC 1. Partition by `"p_eventdate"`.

# COMMAND ----------

# ANSWER
(
    silver_health_tracker_clean.select("device_id", "steps", "eventtime", "name", "p_eventdate")
    .write.format("delta")
    .mode("append")
    .partitionBy("p_eventdate")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS health_tracker_classic_silver
"""
)

spark.sql(
    f"""
CREATE TABLE health_tracker_classic_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使用断言验证模式

# COMMAND ----------

silverTable = spark.read.table("health_tracker_classic_silver")
expected_schema = """
  device_id INTEGER,
  steps INTEGER,
  eventtime TIMESTAMP,
  name STRING,
  p_eventdate DATE
"""

assert silverTable.schema == _parse_datatype_string(expected_schema), "Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM health_tracker_classic_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 更新青铜表以体现加载
# MAGIC 
# MAGIC **练习:**更新青铜表中的记录以反映更新。
# MAGIC 
# MAGIC ### Step 1: 更新干净的记录
# MAGIC 已经加载到银级别表的干净记录，并且应该将其青铜表的`status`更新为`"loaded"`。
# MAGIC 
# MAGIC 💃🏽 **提示**你正在将银级别的dataframe中的`value`列与青铜表中的`value`列进行匹配。

# COMMAND ----------

# ANSWER
from delta.tables import DeltaTable

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_health_tracker_clean.withColumn("status", lit("loaded"))

update_match = "bronze.value = clean.value"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC **练习:**更新青铜表中的记录以反映更新。
# MAGIC 
# MAGIC ### Step 2: 更新隔离记录
# MAGIC 隔离记录的青铜表“status”应更新为“quarantined”。
# MAGIC 
# MAGIC 🕺🏻 **提示**你正在将隔离的银级别dataframe中的`value`列与青铜表中的`value`列进行匹配。

# COMMAND ----------

# ANSWER
silverAugmented = silver_health_tracker_quarantine.withColumn("status", lit("quarantined"))

update_match = "bronze.value = quarantine.value"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

