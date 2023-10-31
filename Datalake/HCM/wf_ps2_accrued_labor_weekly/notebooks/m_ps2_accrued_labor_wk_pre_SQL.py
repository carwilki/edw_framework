# Databricks notebook source
# Code converted on 2023-09-06 09:50:23
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

# Processing node SQ_Shortcut_to_EMPLOYEE_PROFILE_WK, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = spark.sql(f"""SELECT CURRENT_TIMESTAMP AS START_TSTMP,

       'PS2_ACCRUED_LABOR_WK_PRE' AS TABLE_NAME,

       COUNT(*) AS BEGIN_ROW_CNT

  FROM {raw}.PS2_ACCRUED_LABOR_WK_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = SQ_Shortcut_to_EMPLOYEE_PROFILE_WK \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[0],'START_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[1],'TABLE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[2],'BEGIN_ROW_CNT')

# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

SQL_INS_and_DUPS_CHECK2 = spark.sql(f"""INSERT INTO {raw}.PS2_ACCRUED_LABOR_WK_PRE
SELECT
 ps2.week_dt
,ps2.location_id
,ps2.employee_id
,ps2.earn_id
,ps2.store_dept_nbr
,NVL(ps2.job_code,0)
,ps2.fullpt_flag
,ps2.pay_freq_cd
,ps2.hours_worked
,ps2.earnings_amt
,ps2.earnings_loc_amt
,ps2.currency_nbr
FROM
 (
  SELECT
   date_add(ps_pre.week_dt,7)             WEEK_DT
  ,ps_pre.location_id
  ,ps_pre.employee_id
  ,ps_pre.earn_id
  ,ps_pre.store_dept_nbr
  ,ps_pre.job_code
  ,ps_pre.fullpt_flag
  ,ps_pre.pay_freq_cd
  ,(ps_pre.hours_worked * -1)     HOURS_WORKED
  ,(ps_pre.earnings_amt * -1)     EARNINGS_AMT
  ,(ps_pre.earnings_loc_amt * -1) EARNINGS_LOC_AMT
  ,ps_pre.currency_nbr
  FROM
   {raw}.ps2_accrued_labor_wk_pre PS_PRE
  LEFT OUTER JOIN {empl_protected}.raw_ps2_empl_empl_loc_wk_pre PRE
    ON date_add(ps_pre.week_dt,7)    = pre.week_dt
   AND ps_pre.location_id    = pre.location_id
   AND ps_pre.employee_id    = pre.employee_id
   AND ps_pre.earn_id        = pre.earn_id
   AND ps_pre.store_dept_nbr = pre.store_dept_nbr
   AND ps_pre.job_code       = pre.job_code
  WHERE pre.week_dt IS NULL
  UNION
  SELECT
   pre.week_dt
  ,pre.location_id
  ,pre.employee_id
  ,pre.earn_id
  ,pre.store_dept_nbr
  ,pre.job_code
  ,pre.fullpt_flag
  ,CASE WHEN pre.pay_freq_cd = 'H'
        THEN 'W'
        ELSE NVL(pre.pay_freq_cd, 'B')
   END                                   PAY_FREQ_CD
  ,CASE WHEN ps_pre.week_dt IS NOT NULL
        THEN (pre.hours_worked- ps_pre.hours_worked)
        ELSE pre.hours_worked
   END                                   HOURS_WORKED
  ,CASE WHEN ps_pre.week_dt IS NOT NULL
        THEN (pre.earnings_amt- ps_pre.earnings_amt)
        ELSE pre.earnings_amt
   END                                   EARNINGS_AMT
  ,CASE WHEN ps_pre.week_dt IS NOT NULL
        THEN (pre.earnings_loc_amt- ps_pre.earnings_loc_amt)
        ELSE pre.earnings_loc_amt
   END                                   EARNINGS_LOC_AMT
  ,pre.currency_nbr
  FROM
   {empl_protected}.raw_ps2_empl_empl_loc_wk_pre PRE
  LEFT OUTER JOIN {raw}.ps2_accrued_labor_wk_pre PS_PRE
    ON pre.week_dt        = date_add(ps_pre.week_dt,7)
   AND pre.location_id    = ps_pre.location_id
   AND pre.employee_id    = ps_pre.employee_id
   AND pre.earn_id        = ps_pre.earn_id
   AND pre.store_dept_nbr = ps_pre.store_dept_nbr
   AND pre.job_code       = ps_pre.job_code
 ) PS2
WHERE (ps2.earnings_amt <> 0
   OR  ps2.earnings_loc_amt <> 0)""")


SQL_INS_and_DUPS_CHECK = spark.sql(f"""SELECT COUNT(*) AS DUPLICATE_ROWS
  FROM (SELECT WEEK_DT,
               LOCATION_ID,
               EMPLOYEE_ID,
               EARN_ID,
               STORE_DEPT_NBR,
               JOB_CODE,
               FULLPT_FLAG,
               PAY_FREQ_CD,
               COUNT(*) AS CNT
          FROM {raw}.ps2_accrued_labor_wk_pre
         GROUP BY WEEK_DT,
                  LOCATION_ID,
                  EMPLOYEE_ID,
                  EARN_ID,
                  STORE_DEPT_NBR,
                  JOB_CODE,
                  FULLPT_FLAG,
                  PAY_FREQ_CD) T
 WHERE CNT > 1""")
