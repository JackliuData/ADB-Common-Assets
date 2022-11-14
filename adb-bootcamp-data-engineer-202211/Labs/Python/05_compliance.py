# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 遵守GDPR和CCPA
# MAGIC 
# MAGIC 偶尔，客户决定不希望他们的个人数据存储在您的系统中。这些信息通常来自web表单，或存储在数据库中的电子邮件列表，或其他方式。在本实验室中，为了便于说明，我们将使用两个客户的uuid。
# MAGIC 
# MAGIC 在静态(非流)表中，删除数据相对简单直接。在这个实验中，我们将从静态Delta表中删除选择的客户数据。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 笔记本目标
# MAGIC 在这个笔记本中:
# MAGIC 1. 使用“MERGE”删除客户数据
# MAGIC 1. 使用Delta table历史记录验证删除
# MAGIC 1. 使用时间旅行回滚删除
# MAGIC 1. 对表进行Vacuum操作，完成删除操作

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
# MAGIC 
# MAGIC #### Delete data——简单地从静态表中删除数据
# MAGIC 
# MAGIC 在数据系统中，有以下客户是否要求根据GDPR和CCPA删除他们的数据
# MAGIC 
# MAGIC - `'16b807ac-d9da-11ea-8534-0242ac110002'`
# MAGIC - `'16b81c2e-d9da-11ea-8534-0242ac110002'`
# MAGIC 
# MAGIC 这些用户id在`deletions`表中可用:

# COMMAND ----------

display(
    spark.sql(
        """
SELECT (*)
FROM deletions
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 使用Spark SQL操作Delta tables
# MAGIC 我们将通过合并`deletions`表和以下表来从表中删除这些用户:
# MAGIC - `health_tracker_user`
# MAGIC - `health_tracker_classic_bronze`
# MAGIC - `health_tracker_classic_silver`

# COMMAND ----------

spark.sql(
    """
MERGE INTO health_tracker_classic_bronze
USING deletions
ON health_tracker_classic_bronze.value RLIKE deletions.user_id
WHEN MATCHED THEN DELETE
"""
)

# COMMAND ----------

spark.sql(
    """
CREATE OR REPLACE TEMPORARY VIEW deletion_users AS
  SELECT health_tracker_user.user_id, device_id FROM
  deletions JOIN health_tracker_user
  ON deletions.user_id = health_tracker_user.user_id
"""
)

spark.sql(
    """
MERGE INTO health_tracker_classic_silver
USING deletion_users
ON deletion_users.device_id = health_tracker_classic_silver.device_id
WHEN MATCHED THEN DELETE
"""
)

# COMMAND ----------

spark.sql(
    """
MERGE INTO health_tracker_user
USING deletions
ON deletions.user_id = health_tracker_user.user_id
WHEN MATCHED THEN DELETE
"""
)

# COMMAND ----------

display(
    spark.sql(
        """
DESCRIBE HISTORY health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **练习:** 显示前一个版本的计数
# MAGIC 
# MAGIC 从上面的单元格中引用表的历史，以识别以前的版本号。使用它来查询当前版本的`health_tracker_classic_silver`的计数。

# COMMAND ----------

# ANSWER
display(
    spark.sql(
        """
SELECT COUNT(*) FROM health_tracker_classic_silver VERSION AS OF 4
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **练习:** 显示`health_tracker_classic_silver`的当前计数。

# COMMAND ----------

# ANSWER
display(
    spark.sql(
        """
SELECT COUNT(*) FROM health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rollback
# MAGIC 
# MAGIC 哦!您发现在上次提交中有一个客户被错误地删除了。您需要恢复到仅针对该客户记录的表的前一个版本，同时保留为另一个客户所做的更改
# MAGIC 
# MAGIC 🆘 回想一下，`delete`从最新版本的Delta table中删除数据，但在显式地清除旧版本之前，不会将其从物理存储中删除。这将允许我们恢复刚刚删除的数据，但这意味着我们还没有遵守规定。我们稍后会解决这个问题。

# COMMAND ----------

spark.sql(
    """
INSERT INTO health_tracker_classic_silver
SELECT * FROM health_tracker_classic_silver VERSION AS OF 4
WHERE name = 'Simone Graber'
"""
)

# COMMAND ----------

display(
    spark.sql(
        """
SELECT COUNT(*) FROM health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

display(
    spark.sql(
        """
DESCRIBE HISTORY health_tracker_classic_silver
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 检查客户数据是否已经恢复:

# COMMAND ----------

display(
    spark.sql(
        """
SELECT * FROM health_tracker_classic_silver
WHERE name = 'Simone Graber'
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 检查以确保其他客户的数据仍然被删除:

# COMMAND ----------

display(
    spark.sql(
        """
SELECT * FROM health_tracker_classic_silver
WHERE name = 'Julian Andersen'
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vacuum
# MAGIC 
# MAGIC 如果我们查询早期版本的`health_tracker_classic_silver`表，我们可以看到客户的数据仍然存在，因此我们实际上还没有遵守规定。

# COMMAND ----------

display(
    spark.sql(
        """
SELECT * FROM health_tracker_classic_silver VERSION AS OF 4
WHERE name in ('Julian Andersen', 'Simone Graber')
"""
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC 我们可以使用VACUUM命令删除旧文件。VACUUM命令递归地清除与增量表关联的目录，并删除不再处于该表事务日志最新状态且超过保留阈值的文件。默认阈值为7天。

# COMMAND ----------

from pyspark.sql.utils import IllegalArgumentException

try:
    spark.sql(
        """
    VACUUM health_tracker_classic_silver RETAIN 0 Hours
    """
    )
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC 为了立即删除有问题的文件，我们将保留期设置为0小时。不建议将保存间隔设置为少于7天，从错误消息中我们可以看到，有保护措施可以防止此操作成功。
# MAGIC 
# MAGIC 我们将设置Delta来允许这个操作。

# COMMAND ----------

spark.sql(
    """
SET spark.databricks.delta.retentionDurationCheck.enabled = false
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 重新运行vacuum命令:

# COMMAND ----------

spark.sql(
    """
VACUUM health_tracker_classic_silver RETAIN 0 Hours
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC 现在，当我们试图查询更早的版本时，我们会得到一个错误。较早版本的文件已被删除。

# COMMAND ----------

# ANSWER
# Uncomment and run this query
# display(
#     spark.sql(
#         """
# SELECT * FROM health_tracker_classic_silver VERSION AS OF 4
# """
#     )
# )


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>

# COMMAND ----------

