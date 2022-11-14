# Databricks notebook source
# MAGIC %md
# MAGIC ### 生成原始文件
# MAGIC 
# MAGIC 我们编写了一些函数来生成内容随机的原始json文件，并将它们存储到dbfs_location中的一个目录中。
# MAGIC 
# MAGIC 这模拟了原始文件进入ADLS进程的过程。

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text('dbfs_location','dbfs:/tmp/generated_files')
dbutils.widgets.text('num_records','3')
dbfs_location = dbutils.widgets.get('dbfs_location')
num_records = int(dbutils.widgets.get('num_records'))


# COMMAND ----------

# 导包
import json, string, random
from random import randint
import pandas as pd

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

# COMMAND ----------

def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def gen_single_json_str_payload():
    # simulate sensitive content, encryption happen at payload level
    sensitive_key = 'name'
    sensitive_val = id_generator()
    payload_dict = {
      sensitive_key: sensitive_val,
      'age': randint(1, 100)
    }
    return json.dumps(payload_dict)

def gen_single_record():
    return [id_generator(), randint(1, 100)]

def gen_pandas_df(rows: int) -> pd.DataFrame:
    full_list = []
    for i in range(rows):
        full_list.append(gen_single_record())
    return pd.DataFrame(full_list, columns=['name','age']) 

def gen_payload_df(payload_json_str):
    payload_df = spark.createDataFrame([(1, payload_json_str)],["id","body"])
    return payload_df

# COMMAND ----------

# generate list of json files into dbfs, each is a json srting containing only 1 record  
for i in range(num_records):
    df = gen_pandas_df(1)
    df.to_json(f'local_{i}.json')
    dbutils.fs.mv(f"file:///databricks/driver/local_{i}.json", f"{dbfs_location}/local_{i}.json")

# COMMAND ----------

display(dbutils.fs.ls(dbfs_location))

# COMMAND ----------

