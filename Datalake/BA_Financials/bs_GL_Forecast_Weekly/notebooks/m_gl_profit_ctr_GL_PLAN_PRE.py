# Databricks notebook source
#Code converted on 2023-08-17 15:37:00
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

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_To_GL_PLAN_PRE, type SOURCE 
# COLUMN COUNT: 4

_sql = f"""
SELECT NEW_PC.PROFIT_CTR,
       NVL(L.LOCATION_ID,90000)  AS LOCATION_ID,
       NVL(L.COUNTRY_CD,'US') AS COUNTRY_CD,
       CURRENT_DATE
FROM (
SELECT DISTINCT P.STORE_NBR,
                CASE WHEN P.STORE_NBR  > 8000  AND P.STORE_NBR  < 9000 THEN '000000' || LTRIM(TO_CHAR(P.STORE_NBR, '0000'))
                     ELSE LTRIM(TO_CHAR(P.STORE_NBR, '0000')) || '-' || P.GL_CAT_CD
                END AS PROFIT_CTR
FROM {raw}.GL_PLAN_PRE P LEFT OUTER JOIN  {legacy}.GL_PROFIT_CENTER PC  ON       
        CASE WHEN P.STORE_NBR  > 8000  AND P.STORE_NBR  < 9000 THEN '000000' || LTRIM(TO_CHAR(P.STORE_NBR, '0000'))
             ELSE LTRIM(TO_CHAR(P.STORE_NBR, '0000')) || '-' || P.GL_CAT_CD
        END  = PC.GL_PROFIT_CTR_CD
WHERE PC.LOAD_DT  IS NULL  ) NEW_PC 
  LEFT OUTER JOIN {legacy}.SITE_PROFILE L ON  NEW_PC.STORE_NBR = L.STORE_NBR
"""

SQ_Shortcut_To_GL_PLAN_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_To_GL_PLAN_PRE = SQ_Shortcut_To_GL_PLAN_PRE \
	.withColumnRenamed(SQ_Shortcut_To_GL_PLAN_PRE.columns[0],'PROFIT_CTR') \
	.withColumnRenamed(SQ_Shortcut_To_GL_PLAN_PRE.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_GL_PLAN_PRE.columns[2],'COUNTRY_CD') \
	.withColumnRenamed(SQ_Shortcut_To_GL_PLAN_PRE.columns[3],'LOAD_DT')


# COMMAND ----------

# Processing node INSERT_PROFIT_CENTER, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

INSERT_PROFIT_CENTER = SQ_Shortcut_To_GL_PLAN_PRE.selectExpr(
	"PROFIT_CTR as GL_PROFIT_CTR_CD",
	"'UNKNOWN' as GL_PROFIT_CTR_DESC",
	"LOCATION_ID as LOCATION_ID",
	"LOAD_DT as VALID_FROM_DT",
	"TO_DATE('12319999', 'MMddyyyy') as EXP_DT",
	"concat(RTRIM(COUNTRY_CD) , 'D' ) as CURRENCY_ID",
	"LOAD_DT as LOAD_DT").withColumn('pyspark_data_action', lit(0))


# COMMAND ----------

# Processing node Shortcut_To_GL_PROFIT_CENTER1, type TARGET 
# COLUMN COUNT: 7


Shortcut_To_GL_PROFIT_CENTER1 = INSERT_PROFIT_CENTER.selectExpr(
	"CAST(GL_PROFIT_CTR_CD AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(GL_PROFIT_CTR_DESC AS STRING) as GL_PROFIT_CTR_DESC",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(VALID_FROM_DT AS TIMESTAMP) as VALID_FROM_DT",
	"CAST(EXP_DT AS TIMESTAMP) as EXP_DT",
	"CAST(CURRENCY_ID AS STRING) as CURRENCY_ID",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
  "pyspark_data_action"
)

try:
	primary_key = """source.GL_PROFIT_CTR_CD = target.GL_PROFIT_CTR_CD"""
	refined_perf_table = f"{legacy}.GL_PROFIT_CENTER"
	executeMerge(Shortcut_To_GL_PROFIT_CENTER1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_PROFIT_CENTER", "GL_PROFIT_CENTER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_PROFIT_CENTER", "GL_PROFIT_CENTER","Failed",str(e), f"{raw}.log_run_details", )
	raise e
