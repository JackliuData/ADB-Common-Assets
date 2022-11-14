# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 原始数据生成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 笔记本目标
# MAGIC 
# MAGIC 在这个笔记本中:
# MAGIC 1. 从远程数据源采集数据到我们的源目录`rawPath`。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 配置
# MAGIC 
# MAGIC 在运行这个单元格之前，请确保在文件中添加一个唯一的用户名
# MAGIC 
# MAGIC 
# MAGIC `includes/configuration`, e.g.
# MAGIC 
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## 功能函数
# MAGIC 
# MAGIC 运行以下命令加载功能函数' retrieve_data '。

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## 生成用户表
# MAGIC 
# MAGIC 运行下面的单元格以生成用户维度表。

# COMMAND ----------

# MAGIC %run ./includes/user

# COMMAND ----------

# MAGIC %md
# MAGIC ## 使笔记本具有一致性
# MAGIC 每次运行保持一直，删除原先文件

# COMMAND ----------

dbutils.fs.rm(classicPipelinePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 准备实验数据
# MAGIC 
# MAGIC 运行这个单元格来准备我们将用于这个实验室的数据。

# COMMAND ----------

prepare_activity_data(landingPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 摄取函数
# MAGIC `includes/utilities`文件中包含一个名为`ingest_classic_data()`的函数。我们将使用这个函数一次采集一个小时的数据。这将模拟Kafka。
# MAGIC 
# MAGIC 在这里运行这个函数来采集第一个小时的数据。
# MAGIC 
# MAGIC 如果成功，你应该看到结果`True`。

# COMMAND ----------

ingest_classic_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 显示原始数据目录
# MAGIC 你应该看到Raw Data目录中有一个文件。

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC **练习:**使用utility函数获取5个小时的数据，
# MAGIC 
# MAGIC 😎 **注意**函数可以接受一个`hours`参数。

# COMMAND ----------

# ANSWER
ingest_classic_data(hours=5)

# COMMAND ----------

display(dbutils.fs.ls(rawPath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 打印原始文件的内容
# MAGIC **练习**:添加正确的文件路径以显示你加载的两个原始文件的内容。

# COMMAND ----------

# ANSWER
print(
    dbutils.fs.head(
        dbutils.fs.ls("dbfs:/dbacademy/ljc1test/dataengineering/classic/raw/")[0].path
    )
)

# COMMAND ----------

# ANSWER
print(
    dbutils.fs.head(
        dbutils.fs.ls("dbfs:/dbacademy/ljc1test/dataengineering/classic/raw/")[1].path
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 你注意到数据有什么不同吗?(滚动)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <h2 style="color:red">Instructor Note</h2>
# MAGIC 
# MAGIC 
# MAGIC `"name":"Gonzalo Valdés"`的`device_id`作为uuid被传递，而它本应该被传递作为一个字符串编码的整数。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

