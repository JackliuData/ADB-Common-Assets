# Databricks notebook source
# MAGIC %md
# MAGIC ### 原始数据到青铜层
# MAGIC 该notebook将从raw landing zone读取文件，并将文件摄取到青铜层delta table中。
# MAGIC 
# MAGIC 青铜层将是唯一的贴源层。

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('raw_location','dbfs:/tmp/generated_files/local_0.json')
dbutils.widgets.text('db_name','testdb')
dbutils.widgets.text('bronzePath','dbfs:/tmp/testoutput')

raw_location = dbutils.widgets.get('raw_location')
db_name = dbutils.widgets.get('db_name')
bronzePath = dbutils.widgets.get('bronzePath')

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, lit

raw_df = spark.read.json(raw_location)

raw_df_with_metadata = raw_df.select(
    "name",
    "age",
    lit("files.training.databricks.com").alias("datasource"),
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
)

raw_df_with_metadata = raw_df_with_metadata.select("*", 
                            col("ingestdate").alias("p_ingestdate"))

# COMMAND ----------

raw_df_with_metadata.show(10,False)

# COMMAND ----------

raw_df_with_metadata.write.format("delta").mode("append").partitionBy("p_ingestdate").save(bronzePath)

# COMMAND ----------

sdf = spark.read.format('delta').load(bronzePath)

# COMMAND ----------

print(sdf.count())

# COMMAND ----------

display(sdf)

# COMMAND ----------

