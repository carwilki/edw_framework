# Databricks notebook source
#Code converted on 2023-08-09 13:02:54
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")




if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# COMMAND ----------

# Processing node ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE, type SOURCE 
# COLUMN COUNT: 7

_sql = f"""
SELECT
    pc.cost_center_id,
    COALESCE(pc.gl_profit_ctr_desc, s.store_name) AS GL_PROFIT_CTR_DESC,
    CASE
        WHEN pc.location_id <> 90000 THEN pc.location_id
        WHEN pc.location_id = 90000 THEN COALESCE(s.location_id, 90000)
        WHEN pc.location_id IS NULL THEN 90000
    END AS LOCATION_ID,
    pc.valid_from_dt,
    pc.exp_dt,
    COALESCE(pc.currency_id, RTRIM(s.country_cd) || 'D') AS CURRENCY_ID,
    DATE_TRUNC('DAY', CURRENT_DATE) AS CURRENT_DATE
FROM (
    SELECT
        RTRIM(csks.cost_center_id) AS COST_CENTER_ID,
        MAX(p.gl_profit_ctr_desc) AS GL_PROFIT_CTR_DESC,
        MAX(p.location_id) AS LOCATION_ID,
        MAX(csks.valid_from_dt) AS VALID_FROM_DT,
        MAX(csks.exp_dt) AS EXP_DT,
        MAX(SUBSTR(csks.currency_cd, 1, 3)) AS CURRENCY_ID
    FROM {raw}.gl_csks_cost_ctr_pre CSKS
    LEFT OUTER JOIN {legacy}.gl_profit_center P ON RTRIM(csks.cost_center_id) = p.gl_profit_ctr_cd
    WHERE csks.co_area_id = 'PETM'
    GROUP BY RTRIM(csks.cost_center_id)
) PC
LEFT OUTER JOIN {legacy}.site_profile S
ON CASE
    WHEN SUBSTR(pc.cost_center_id, 1, 4) = '0000' AND SUBSTR(pc.cost_center_id, 7, 1) <> '3'
        THEN CAST(SUBSTR(pc.cost_center_id, 7, 4) AS NUMERIC)
    WHEN SUBSTR(pc.cost_center_id, 1, 4) = '0000' AND SUBSTR(pc.cost_center_id, 7, 1) = '3'
        THEN CAST(SUBSTR(pc.cost_center_id, 9, 2) AS NUMERIC)
    WHEN SUBSTR(pc.cost_center_id, 5, 1) = '-'
        THEN CAST(SUBSTR(pc.cost_center_id, 1, 4) AS NUMERIC)
END = s.store_nbr
"""

ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE = ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[0],'GL_PROFIT_CTR_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[1],'GL_PROFIT_CTR_DESC') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[2],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[3],'VALID_FROM_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[4],'EXP_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[5],'CURRENCY_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.columns[6],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_To_GL_PROFIT_CENTER1, type TARGET 
# COLUMN COUNT: 7


Shortcut_To_GL_PROFIT_CENTER1 = ASQ_Shortcut_To_GL_CSKS_COST_CTR_PRE.selectExpr(
	"CAST(GL_PROFIT_CTR_CD AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(GL_PROFIT_CTR_DESC AS STRING) as GL_PROFIT_CTR_DESC",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(VALID_FROM_DT AS TIMESTAMP) as VALID_FROM_DT",
	"CAST(EXP_DT AS TIMESTAMP) as EXP_DT",
	"CAST(CURRENCY_CD AS STRING) as CURRENCY_ID",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)

# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.GL_PROFIT_CENTER"
  Shortcut_To_GL_PROFIT_CENTER1.createOrReplaceTempView('temp_profit_center')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_profit_center as source
                  ON source.GL_PROFIT_CTR_CD = target.GL_PROFIT_CTR_CD
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e
