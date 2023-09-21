# Databricks notebook source
#Code converted on 2023-08-17 15:37:08
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

# Processing node SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH, type SOURCE 
# COLUMN COUNT: 13

_sql = f"""
SELECT
    PRE.FISCAL_MO,
    PRE.GL_ACCT_NBR,
    PRE.GL_CATEGORY_CD,
    PRE.PROFIT_CTR,
    PRE.LOCATION_ID,
    PRE.CURR_CD,
    COALESCE(GPFM.GL_PLAN_AMT_LOC, 0) AS GL_PLAN_AMT_LOC,
    COALESCE(GPFM.GL_PLAN_AMT_US, 0)  AS GL_PLAN_AMT_US,
    PRE.GL_FORECAST_AMT_LOC,
    PRE.GL_FORECAST_AMT_US,
    CURRENT_DATE AS UPDATE_DT,
    COALESCE(GPFM.LOAD_DT, CURRENT_DATE) AS LOAD_DT,
    CASE WHEN GPFM.FISCAL_MO IS NULL THEN 0 /* INSERT */
         ELSE 1 /* UPDATE */
    END AS UPDATE_FLAG
FROM {raw}.GL_FORECAST_MONTH_PRE PRE
LEFT OUTER JOIN {legacy}.GL_PLAN_FORECAST_MONTH GPFM
     ON PRE.FISCAL_MO           = GPFM.FISCAL_MO
    AND PRE.GL_ACCT_NBR         = GPFM.GL_ACCT_NBR
    AND PRE.GL_CATEGORY_CD      = GPFM.GL_CATEGORY_CD
    AND PRE.PROFIT_CTR          = GPFM.GL_PROFIT_CTR_CD
WHERE (GPFM.LOCATION_ID IS NULL
       AND COALESCE(PRE.GL_FORECAST_AMT_US, 0) <> 0)
   OR (GPFM.LOCATION_ID IS NOT NULL
       AND (COALESCE(PRE.GL_FORECAST_AMT_LOC, 0) <> COALESCE(GPFM.GL_FORECAST_AMT_LOC, 0)
            OR COALESCE(PRE.GL_FORECAST_AMT_US, 0) <> COALESCE(GPFM.GL_FORECAST_AMT_US, 0)))
"""

SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH = SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[0],'FISCAL_MO') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[1],'GL_ACCT_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[2],'GL_CATEGORY_CD') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[3],'GL_PROFIT_CTR_CD') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[4],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[5],'LOC_CURRENCY_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[6],'GL_PLAN_AMT_LOC') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[7],'GL_PLAN_AMT_US') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[8],'GL_FORECAST_AMT_LOC') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[9],'GL_FORECAST_AMT_US') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[10],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[11],'LOAD_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[12],'UPDATE_FLAG')

# COMMAND ----------

# Processing node UPD_Insert_Update, type UPDATE_STRATEGY 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_temp = SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.toDF(*["SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___" + col for col in SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns])

UPD_Insert_Update = SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_temp.selectExpr(
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___FISCAL_MO as FISCAL_MO",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_ACCT_NBR as GL_ACCT_NBR",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_CATEGORY_CD as GL_CATEGORY_CD",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_PROFIT_CTR_CD as GL_PROFIT_CTR_CD",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___LOC_CURRENCY_ID as LOC_CURRENCY_ID",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_PLAN_AMT_LOC as GL_PLAN_AMT_LOC",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_PLAN_AMT_US as GL_PLAN_AMT_US",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_FORECAST_AMT_LOC as GL_FORECAST_AMT_LOC",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___GL_FORECAST_AMT_US as GL_FORECAST_AMT_US",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___LOAD_DT as LOAD_DT",
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', col("UPDATE_FLAG"))

# COMMAND ----------

# Processing node Shortcut_to_GL_PLAN_FORECAST_MONTH_Ins_Upd, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_GL_PLAN_FORECAST_MONTH_Ins_Upd = UPD_Insert_Update.selectExpr(
	"CAST(FISCAL_MO AS INT) as FISCAL_MO",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(GL_PROFIT_CTR_CD AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(LOC_CURRENCY_ID AS STRING) as LOC_CURRENCY_ID",
	"CAST(GL_PLAN_AMT_LOC AS DECIMAL(15,2)) as GL_PLAN_AMT_LOC",
	"CAST(GL_PLAN_AMT_US AS DECIMAL(15,2)) as GL_PLAN_AMT_US",
	"CAST(GL_FORECAST_AMT_LOC AS DECIMAL(15,2)) as GL_FORECAST_AMT_LOC",
	"CAST(GL_FORECAST_AMT_US AS DECIMAL(15,2)) as GL_FORECAST_AMT_US",
	"CAST(NULL AS DECIMAL(15,2)) as GL_F1_ADJ_AMT_LOC",
	"CAST(NULL AS DECIMAL(15,2)) as GL_F1_ADJ_AMT_US",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.FISCAL_MO = target.FISCAL_MO AND source.GL_ACCT_NBR = target.GL_ACCT_NBR AND source.GL_CATEGORY_CD = target.GL_CATEGORY_CD AND source.GL_PROFIT_CTR_CD = target.GL_PROFIT_CTR_CD"""
	refined_perf_table = f"{legacy}.GL_PLAN_FORECAST_MONTH"
	executeMerge(Shortcut_to_GL_PLAN_FORECAST_MONTH_Ins_Upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_PLAN_FORECAST_MONTH", "GL_PLAN_FORECAST_MONTH", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_PLAN_FORECAST_MONTH", "GL_PLAN_FORECAST_MONTH","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		
