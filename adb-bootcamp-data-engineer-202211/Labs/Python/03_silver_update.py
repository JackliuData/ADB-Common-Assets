# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 银级别表更新
# MAGIC 
# MAGIC 我们已经处理了从Bronze表到Silver表的数据。
# MAGIC 
# MAGIC 现在我们需要进行一些更新，以确保Silver表中的高数据质量。因为批处理加载没有用于检查点的机制，所以我们需要一种只从Bronze表加载新记录的方法。
# MAGIC 
# MAGIC 我们还需要处理隔离记录。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 笔记本目标
# MAGIC 
# MAGIC 在这个笔记本中:
# MAGIC 1. 更新`read_batch_bronze`函数，使其只读取新记录
# MAGIC 1. 修复Bronze表中错误的隔离记录
# MAGIC 1. 将修复的记录写入Silver表

# COMMAND ----------

# MAGIC %md
# MAGIC ## 配置

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 导入功能函数

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### 加载更多原始数据

# COMMAND ----------

ingest_classic_data(hours=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta当前架构
# MAGIC 接下来，我们在Delta架构中演示到目前为止构建的所有内容。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 原始数据到青铜层管道

# COMMAND ----------

rawDF = read_batch_raw(spark, rawPath)
transformedRawDF = transform_raw(rawDF)
rawToBronzeWriter = batch_writer(
    dataframe=transformedRawDF, partition_column="p_ingestdate"
)

rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 清除原始文件路径
# MAGIC 
# MAGIC 手动清除已经加载的原始文件。

# COMMAND ----------

# ANSWER
dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 从青铜层到银层的管道
# MAGIC 
# MAGIC 
# MAGIC 在前面的notebook中，只采集我们运行的新数据
# MAGIC 
# MAGIC ```
# MAGIC bronzeDF = (
# MAGIC   spark.read
# MAGIC   .table("health_tracker_classic_bronze")
# MAGIC   .filter("status = 'new'")
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC **练习**
# MAGIC 
# MAGIC 更新函数`includes/main/python/operations`中的`read_batch_bronze`函数，这样它就只读取Bronze表中的新文件。

# COMMAND ----------

# MAGIC %md
# MAGIC ♨️ 更新`read_batch_bronze`函数后，通过运行下面的单元格，重新启动`includes/main/python/operations`文件来包含你的更新。

# COMMAND ----------

# MAGIC %run ./includes/main/python/operations

# COMMAND ----------

bronzeDF = read_batch_bronze(spark, bronzePath)
transformedBronzeDF = transform_bronze(bronzeDF)

(silverCleanDF, silverQuarantineDF) = generate_clean_and_quarantine_dataframes(
    transformedBronzeDF
)

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanDF, partition_column="p_eventdate", exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silverCleanDF, "loaded")
update_bronze_table_status(spark, bronzePath, silverQuarantineDF, "quarantined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 对银表进行目视验证

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM health_tracker_classic_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 处理隔离记录

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: 从青铜表加载隔离记录

# COMMAND ----------

# MAGIC %md
# MAGIC **练习**
# MAGIC 
# MAGIC 从状态为“quarantined”的青铜表中加载所有记录。

# COMMAND ----------

# ANSWER

bronzeQuarantinedDF = spark.read.table("health_tracker_classic_bronze").filter(
    "status = 'quarantined'"
)
display(bronzeQuarantinedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: 转换隔离记录
# MAGIC 
# MAGIC 这适用于标准的青铜表转换。

# COMMAND ----------

bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
    "quarantine"
)
display(bronzeQuarTransDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: 将隔离的数据与用户数据关联
# MAGIC 
# MAGIC 我们这样做是为了检索与每个用户相关联的正确设备id。

# COMMAND ----------

health_tracker_user_df = spark.read.table("health_tracker_user").alias("user")
repairDF = bronzeQuarTransDF.join(
    health_tracker_user_df,
    bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
)
display(repairDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: 从已连接的`user` DataFrame中选择正确的设备

# COMMAND ----------

silverCleanedDF = repairDF.select(
    col("quarantine.value").alias("value"),
    col("user.device_id").cast("INTEGER").alias("device_id"),
    col("quarantine.steps").alias("steps"),
    col("quarantine.eventtime").alias("eventtime"),
    col("quarantine.name").alias("name"),
    col("quarantine.eventtime").cast("date").alias("p_eventdate"),
)
display(silverCleanedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: 批量将已修复(以前已隔离)的记录写入银级别表
# MAGIC 
# MAGIC 加载后，这还将隔离记录的状态更新为“loaded”。

# COMMAND ----------

bronzeToSilverWriter = batch_writer(
    dataframe=silverCleanedDF, partition_column="p_eventdate", exclude_columns=["value"]
)
bronzeToSilverWriter.save(silverPath)

update_bronze_table_status(spark, bronzePath, silverCleanedDF, "loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 显示隔离记录
# MAGIC 
# MAGIC 如果更新成功，青铜表中应该没有隔离记录。

# COMMAND ----------

display(bronzeQuarantinedDF)


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

