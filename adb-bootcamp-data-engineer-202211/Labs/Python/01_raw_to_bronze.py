# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 原始数据到青铜层

# COMMAND ----------

# MAGIC %md
# MAGIC ## 笔记本目标
# MAGIC 
# MAGIC 在这个笔记本中:
# MAGIC 
# MAGIC 1. 采集原始数据
# MAGIC 1. 使用采集元数据增强数据
# MAGIC 1. 批量写入增强后的数据到青铜表中

# COMMAND ----------

# MAGIC %md
# MAGIC ## 配置

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC 显示原始路径中的文件

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使笔记本具有一致性

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 摄取原始数据
# MAGIC 
# MAGIC 接下来，我们将从源目录读取文件，并将每一行作为字符串写入Bronze表。
# MAGIC 
# MAGIC 🤠 您应该使用spark.read进行批量加载
# MAGIC 
# MAGIC 使用格式`"text"`并使用提供的模式读取。

# COMMAND ----------

# ANSWER
kafka_schema = "value STRING"

raw_health_tracker_data_df = spark.read.format("text").schema(kafka_schema).load(rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 显示原始数据
# MAGIC 
# MAGIC 🤓 这里的每一行都是JSON格式的原始字符串，模拟Kafka这样的流服务器所传递的一样。

# COMMAND ----------

display(raw_health_tracker_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 摄入元数据
# MAGIC 
# MAGIC 作为数据采集过程的一部分，我们记录数据采集所需的元数据。
# MAGIC 
# MAGIC **练习:**向传入的原始数据添加元数据。你应该添加以下列:
# MAGIC 
# MAGIC - data source (`datasource`), use `"files.training.databricks.com"`
# MAGIC - ingestion time (`ingesttime`)
# MAGIC - status (`status`), use `"new"`
# MAGIC - ingestion date (`ingestdate`)

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import current_timestamp, lit

raw_health_tracker_data_df = raw_health_tracker_data_df.select(
    lit("files.training.databricks.com").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    "value",
    current_timestamp().cast("date").alias("ingestdate"),
)

# COMMAND ----------

display(raw_health_tracker_data_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 写批到一个青铜表
# MAGIC 
# MAGIC 最后，我们写入青铜表。
# MAGIC 
# MAGIC 一定要按正确的顺序写 (`"datasource"`, `"ingesttime"`, `"status"`, `"value"`, `"p_ingestdate"`).
# MAGIC 
# MAGIC 请确保使用以下选项:
# MAGIC 
# MAGIC - the format `"delta"`
# MAGIC - using the append mode
# MAGIC - partition by `p_ingestdate`

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import col

(
    raw_health_tracker_data_df.select(
        "datasource",
        "ingesttime",
        "status",
        "value",
        col("ingestdate").alias("p_ingestdate"),
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 把青铜表注册到Metastore
# MAGIC 
# MAGIC 该表应该命名为`health_tracker_classic_bronze`。

# COMMAND ----------

# ANSWER
spark.sql(
    """
DROP TABLE IF EXISTS health_tracker_classic_bronze
"""
)

spark.sql(
    f"""
CREATE TABLE health_tracker_classic_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 显示青铜表
# MAGIC 
# MAGIC 运行这个查询以显示经典青铜表的内容

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM health_tracker_classic_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 查询异常记录
# MAGIC 
# MAGIC 
# MAGIC 运行一个SQL查询，只显示“Gonzalo Valdés”的传入记录。
# MAGIC 
# MAGIC 🧠你可以使用SQL操作符' RLIKE '，它是正则表达式' LIKE '的缩写，来创建匹配的谓词。
# MAGIC 
# MAGIC [`RLIKE` documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/functions.html#rlike)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM health_tracker_classic_bronze WHERE value RLIKE 'Gonzalo Valdés'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 你注意到了什么?

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC 回想一下，在前面的notebook中，我们确定传递了一些device_id作为uuid字符串，而不是字符串编码的整数。
# MAGIC 
# MAGIC 请注意，在一条记录中，设备似乎传递了`user_id`而不是`device_id`。
# MAGIC 
# MAGIC 值得注意的是，我们不会在青铜层处理这个问题。我们只是简单地采集数据。这个问题将在银层解决。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 显示用户维度表
# MAGIC 
# MAGIC 运行SQL查询来显示`health_tracker_user`中的记录。

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM health_tracker_user

# COMMAND ----------

# MAGIC %md
# MAGIC ## 清除原始文件路径
# MAGIC 
# MAGIC 我们使用批量加载的方式加载原始文件，而在Plus流水线中，我们使用流加载
# MAGIC 
# MAGIC 这样做的影响是，batch不使用检查点机制，因此无法知道哪些文件被采集了。
# MAGIC 
# MAGIC 我们需要手动清除已加载的原始文件。

# COMMAND ----------

dbutils.fs.rm(rawPath, recurse=True)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

