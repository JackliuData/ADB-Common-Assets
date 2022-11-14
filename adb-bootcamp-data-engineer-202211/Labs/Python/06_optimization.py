# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Table优化

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
# MAGIC ## 运行原始数据->青铜层->银层的Delta架构

# COMMAND ----------

# MAGIC %run ./04_main

# COMMAND ----------

# MAGIC %md
# MAGIC ## 小文件问题

# COMMAND ----------

# MAGIC %md
# MAGIC #### 显示银级别表分区

# COMMAND ----------

display(dbutils.fs.ls(silverPath))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 显示分区中的文件

# COMMAND ----------

display(dbutils.fs.ls(silverPath + "/p_eventdate=2020-01-02/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE health_tracker_classic_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### 显示分区中的文件

# COMMAND ----------

display(dbutils.fs.ls(silverPath + "/p_eventdate=2020-01-02/"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 您会注意到已经添加了一个文件，现在又多了一个。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 如果你希望某一列在查询谓词中经常使用，并且该列的基数很高(即有大量不同的值)，那么就使用Z-ORDER BY。
# MAGIC 
# MAGIC 您可以为ZORDER BY指定多个列，作为逗号分隔的列表。然而，每增加一列，局部性的有效性就会下降。对于没有收集统计信息的列，z排序将是无效的，并且是资源的浪费，因为数据跳过需要列本地统计信息，如min、max和count。您可以通过重新排序模式中的列或增加要收集统计信息的列的数量来配置某些列上的统计信息收集

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE health_tracker_classic_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### 使用Z-Order优化

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC OPTIMIZE health_tracker_classic_silver
# MAGIC ZORDER BY device_id, steps

# COMMAND ----------

display(dbutils.fs.ls(silverPath + "/p_eventdate=2020-01-02/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE health_tracker_classic_silver

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

