# Databricks notebook source
# Code converted on 2023-09-06 09:51:46
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'


# COMMAND ----------

# Processing node SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE = spark.sql(f"""SELECT pre.week_dt

      ,pre.location_id

      ,pre.earn_id

      ,pre.store_dept_nbr

      ,pre.job_code

      ,pre.act_hours_worked

      ,pre.act_earnings_loc_amt

      ,pre.earned_hours

      ,pre.earned_loc_amt

      ,pre.forecast_hrs

      ,pre.forecast_loc_amt

      ,pre.exchange_rate

      ,CASE WHEN palw.week_dt IS NULL

            THEN 1

            ELSE 0

       END load_flag

      ,CURRENT_DATE AS UPDATE_DT

      ,NVL(palw.LOAD_DT, CURRENT_DATE) AS LOAD_DT

  FROM (SELECT ps_pre.week_dt

              ,sp.location_id

              ,ps_pre.earn_id

              ,ps_pre.store_dept_nbr

              ,ps_pre.job_code

              ,ps_pre.act_hours_worked

              ,ps_pre.act_earnings_loc_amt

              ,ps_pre.earned_hours

              ,ps_pre.earned_loc_amt

              ,ps_pre.forecast_hrs

              ,ps_pre.forecast_loc_amt

              ,CASE WHEN trim(sp.country_cd) = 'CA'

                    THEN NVL (cd.exchange_rate_pcnt, 1)

                    ELSE 1

               END exchange_rate

          FROM {raw}.ps2_adjusted_labor_wk_pre ps_pre

               LEFT OUTER JOIN

               (SELECT   DECODE (date_part ('DOW', cd.day_dt),1, cd.day_dt, (  cd.day_dt- (date_part ('DOW', cd.day_dt) - 1)+ 7) ) week_dt

                        ,MAX (cd.exchange_rate_pcnt) exchange_rate_pcnt

                  FROM {legacy}.currency_day cd

                GROUP BY DECODE (date_part ('DOW', cd.day_dt),1, cd.day_dt, (  cd.day_dt- (date_part ('DOW', cd.day_dt) - 1)+ 7) ) ) cd

                ON ps_pre.week_dt = cd.week_dt

              ,{legacy}.site_profile sp

         WHERE ps_pre.store_nbr = sp.store_nbr) pre

       LEFT OUTER JOIN

       {legacy}.ps2_adjusted_labor_wk palw

       ON pre.week_dt = palw.week_dt

     AND pre.location_id = palw.location_id

     AND pre.earn_id = palw.earn_id

     AND pre.store_dept_nbr = palw.store_dept_nbr

     AND pre.job_code = palw.job_code

    ORDER BY pre.week_dt

          ,pre.location_id""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE = SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[0],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[2],'EARN_ID') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[3],'STORE_DEPT_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[4],'JOB_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[5],'ACT_HOURS_WORKED') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[6],'ACT_EARNINGS_LOC_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[7],'EARNED_HOURS') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[8],'EARNED_LOC_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[9],'FORECAST_HRS') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[10],'FORECAST_LOC_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[11],'EXCHANGE_RATE_PCNT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[12],'LOAD_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[13],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[14],'LOAD_DT')

# COMMAND ----------

# Processing node UPD_ins_upd, type UPDATE_STRATEGY 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE_temp = SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.toDF(*["SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___" + col for col in SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns])

UPD_ins_upd = SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE_temp.selectExpr(
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___EARN_ID as EARN_ID",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___STORE_DEPT_NBR as STORE_DEPT_NBR",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___JOB_CODE as JOB_CODE",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___ACT_HOURS_WORKED as ACT_HOURS_WORKED",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___ACT_EARNINGS_LOC_AMT as ACT_EARNINGS_LOC_AMT",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___EARNED_HOURS as EARNED_HOURS",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___EARNED_LOC_AMT as EARNED_LOC_AMT",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___FORECAST_HRS as FORECAST_HRS",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___FORECAST_LOC_AMT as FORECAST_LOC_AMT",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___EXCHANGE_RATE_PCNT as EXCHANGE_RATE_PCNT",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___LOAD_FLAG as LOAD_FLAG",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___LOAD_DT as LOAD_DT",
	"if(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE___LOAD_FLAG==1,0,1) as pyspark_data_action")

# COMMAND ----------

# Processing node Shortcut_to_PS2_ADJUSTED_LABOR_ins_upd, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_PS2_ADJUSTED_LABOR_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(EARN_ID AS STRING) as EARN_ID",
	"CAST(STORE_DEPT_NBR AS STRING) as STORE_DEPT_NBR",
	"CAST(JOB_CODE AS BIGINT) as JOB_CODE",
	"CAST(ACT_HOURS_WORKED AS DECIMAL(9,2)) as ACT_HOURS_WORKED",
	"CAST(ACT_EARNINGS_LOC_AMT AS DECIMAL(9,2)) as ACT_EARNINGS_LOC_AMT",
	"CAST(EARNED_HOURS AS DECIMAL(9,2)) as EARNED_HOURS",
	"CAST(EARNED_LOC_AMT AS DECIMAL(9,2)) as EARNED_LOC_AMT",
	"CAST(FORECAST_HRS AS DECIMAL(9,2)) as FORECAST_HRS",
	"CAST(FORECAST_LOC_AMT AS DECIMAL(9,2)) as FORECAST_LOC_AMT",
	"CAST(EXCHANGE_RATE_PCNT AS DECIMAL(9,6)) as EXCHANGE_RATE",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.WEEK_DT = target.WEEK_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.EARN_ID = target.EARN_ID AND source.STORE_DEPT_NBR = target.STORE_DEPT_NBR AND source.JOB_CODE = target.JOB_CODE"""
	refined_perf_table = f"{legacy}.PS2_ADJUSTED_LABOR_WK"
	executeMerge(Shortcut_to_PS2_ADJUSTED_LABOR_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("PS2_ADJUSTED_LABOR_WK", "PS2_ADJUSTED_LABOR_WK", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("PS2_ADJUSTED_LABOR_WK", "PS2_ADJUSTED_LABOR_WK","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


