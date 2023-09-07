# Databricks notebook source
#Code converted on 2023-08-09 13:02:34
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

# Processing node SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH, type SOURCE 
# COLUMN COUNT: 12

_sql = f"""
SELECT DISTINCT
    pre.fiscal_mo,
    pre.gl_acct_nbr,
    pre.gl_category_cd,
    pre.profit_ctr AS GL_PROFIT_CTR_CD,
    pre.location_id,
    pre.curr_cd AS LOC_CURRENCY_ID,
    SUM(pre.loc_plan_amt) OVER (PARTITION BY pre.fiscal_mo, pre.gl_acct_nbr, pre.gl_category_cd, pre.profit_ctr, pre.location_id, pre.curr_cd) AS GL_PLAN_AMT_LOC,
    SUM(pre.us_plan_amt) OVER (PARTITION BY pre.fiscal_mo, pre.gl_acct_nbr, pre.gl_category_cd, pre.profit_ctr, pre.location_id, pre.curr_cd) AS GL_PLAN_AMT_US,
    pre.gl_forecast_amt_loc,
    pre.gl_forecast_amt_us,
    CURRENT_DATE AS update_dt,
    pre.load_dt AS load_dt,
    'U' AS row_action
FROM (
    SELECT
        pre.fiscal_mo,
        pre.gl_acct_nbr,
        pre.gl_category_cd,
        pre.profit_ctr,
        pre.location_id,
        pre.curr_cd,
        pre.loc_plan_amt,
        pre.us_plan_amt,
        COALESCE(gpfm.gl_forecast_amt_loc, 0) AS gl_forecast_amt_loc,
        COALESCE(gpfm.gl_forecast_amt_us, 0) AS gl_forecast_amt_us,
        COALESCE(gpfm.load_dt, CURRENT_DATE) AS load_dt
    FROM {raw}.gl_glpct_plan_month_pre pre
    LEFT OUTER JOIN {legacy}.gl_plan_forecast_month gpfm
        ON pre.fiscal_mo = gpfm.fiscal_mo
           AND pre.gl_acct_nbr = gpfm.gl_acct_nbr
           AND pre.gl_category_cd = gpfm.gl_category_cd
           AND pre.profit_ctr = gpfm.gl_profit_ctr_cd
    UNION
    SELECT DISTINCT
        gpfm.fiscal_mo,
        gpfm.gl_acct_nbr,
        gpfm.gl_category_cd,
        gpfm.gl_profit_ctr_cd AS profit_ctr,
        gpfm.location_id,
        gpfm.loc_currency_id AS curr_cd,
        0 AS loc_plan_amt,
        0 AS us_plan_amt,
        COALESCE(gpfm.gl_forecast_amt_loc, 0) AS gl_forecast_amt_loc,
        COALESCE(gpfm.gl_f1_adj_amt_us, 0) AS gl_forecast_amt_us,
        COALESCE(gpfm.load_dt, CURRENT_DATE) AS load_dt
    FROM {legacy}.gl_plan_forecast_month gpfm
    JOIN {raw}.gl_glpct_plan_month_pre pre
        ON gpfm.fiscal_mo = pre.fiscal_mo
           AND gpfm.gl_acct_nbr = pre.gl_acct_nbr
           AND gpfm.location_id = pre.location_id
           AND gpfm.gl_plan_amt_loc > 0
) pre
LEFT OUTER JOIN {legacy}.gl_plan_forecast_month gpfm
    ON pre.fiscal_mo = gpfm.fiscal_mo
       AND pre.gl_acct_nbr = gpfm.gl_acct_nbr
       AND pre.gl_category_cd = gpfm.gl_category_cd
       AND pre.profit_ctr = gpfm.gl_profit_ctr_cd
WHERE loc_plan_amt = 0
"""

SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
# SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH = SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[0],'FISCAL_MO') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[1],'GL_ACCT_NBR') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[2],'GL_CATEGORY_CD') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[3],'GL_PROFIT_CTR_CD') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[4],'LOCATION_ID') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[5],'LOC_CURRENCY_ID') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[6],'GL_PLAN_AMT_LOC') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[7],'GL_PLAN_AMT_US') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[8],'GL_FORECAST_AMT_LOC') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[9],'GL_FORECAST_AMT_US') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[10],'UPDATE_DT') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns[11],'ROW_ACTION')

# COMMAND ----------

print(SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns)

# COMMAND ----------

# Processing node UPD_ins_upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_temp = SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.toDF(*["SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___" + col for col in SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH.columns])

UPD_ins_upd = SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH_temp.selectExpr(
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
	"SQ_Shortcut_to_GL_PLAN_FORECAST_MONTH___ROW_ACTION as ROW_ACTION") \
	.withColumn('LOAD_DT1', expr("NULL")) \
  .withColumn('pyspark_data_action', expr("IF(ROW_ACTION='I', 0, 1)"))

# COMMAND ----------

# Processing node Shortcut_to_GL_PLAN_FORECAST_MONTH_NZ_ins_upd, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_GL_PLAN_FORECAST_MONTH_NZ_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(FISCAL_MO AS BIGINT) as FISCAL_MO",
	"CAST(GL_ACCT_NBR AS BIGINT) as GL_ACCT_NBR",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(GL_PROFIT_CTR_CD AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(LOC_CURRENCY_ID AS STRING) as LOC_CURRENCY_ID",
	"CAST(GL_PLAN_AMT_LOC AS DECIMAL(15,2)) as GL_PLAN_AMT_LOC",
	"CAST(GL_PLAN_AMT_US AS DECIMAL(15,2)) as GL_PLAN_AMT_US",
	"CAST(GL_FORECAST_AMT_LOC AS DECIMAL(15,2)) as GL_FORECAST_AMT_LOC",
	"CAST(GL_FORECAST_AMT_US AS DECIMAL(15,2)) as GL_FORECAST_AMT_US",
	"CAST(NULL AS DECIMAL(15,2)) as GL_F1_ADJ_AMT_LOC",
	"CAST(NULL AS DECIMAL(15,2)) as GL_F1_ADJ_AMT_US",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT1 AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.FISCAL_MO = target.FISCAL_MO AND source.GL_ACCT_NBR = target.GL_ACCT_NBR AND source.GL_CATEGORY_CD = target.GL_CATEGORY_CD AND source.GL_PROFIT_CTR_CD = target.GL_PROFIT_CTR_CD"""
	refined_perf_table = f"{legacy}.GL_PLAN_FORECAST_MONTH"
	executeMerge(Shortcut_to_GL_PLAN_FORECAST_MONTH_NZ_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_PLAN_FORECAST_MONTH", "GL_PLAN_FORECAST_MONTH", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_PLAN_FORECAST_MONTH", "GL_PLAN_FORECAST_MONTH","Failed", str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


