-- Databricks notebook source
-- MAGIC %md # Delta Live Tables 快速入门 (SQL)
-- MAGIC 
-- MAGIC 一个notebook，它提供了一个Delta Live Tables的示例管道:
-- MAGIC 
-- MAGIC - 将原始JSON点击流数据读取到表中。
-- MAGIC - 从原始数据表中读取记录，并使用Delta Live Tables查询和期望来创建一个包含已清理和准备好的数据的新表。
-- MAGIC - 使用Delta Live Tables查询对准备好的数据执行分析。

-- COMMAND ----------

-- DBTITLE 1,采集原始点击流数据
CREATE LIVE TABLE clickstream_raw
COMMENT "The raw wikipedia click stream dataset, ingested from /databricks-datasets."
AS SELECT * FROM json.`/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json`

-- COMMAND ----------

-- DBTITLE 1,清理和准备数据
CREATE LIVE TABLE clickstream_clean(
  CONSTRAINT valid_current_page EXPECT (current_page_title IS NOT NULL),
  CONSTRAINT valid_count EXPECT (click_count > 0) ON VIOLATION FAIL UPDATE
)
COMMENT "Wikipedia clickstream data cleaned and prepared for analysis."
AS SELECT
  curr_title AS current_page_title,
  CAST(n AS INT) AS click_count,
  prev_title AS previous_page_title
FROM live.clickstream_raw

-- COMMAND ----------

-- DBTITLE 1,Top referring pages
CREATE LIVE TABLE top_spark_referers
COMMENT "A table containing the top pages linking to the Apache Spark page."
AS SELECT
  previous_page_title as referrer,
  click_count
FROM live.clickstream_clean
WHERE current_page_title = 'Apache_Spark'
ORDER BY click_count DESC
LIMIT 10

-- COMMAND ----------


