# Databricks notebook source
# Code converted on 2023-09-06 09:50:24
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

       'PS2_ACCRUED_LABOR_WK' AS TABLE_NAME,

       COUNT(*) AS BEGIN_ROW_CNT

  FROM {legacy}.PS2_ACCRUED_LABOR_WK
""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = SQ_Shortcut_to_EMPLOYEE_PROFILE_WK \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[0],'START_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[1],'TABLE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[2],'BEGIN_ROW_CNT')

# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

SQL_INS_and_DUPS_CHECK2 = spark.sql(f"""INSERT OVERWRITE  {legacy}.PS2_ACCRUED_LABOR_WK
SELECT
 palw.week_dt
,palw.location_id
,palw.store_dept_nbr
,palw.earn_id
,palw.job_code
,palw.fullpt_flag
,palw.pay_freq_cd
,SUM(palw.hours_worked)
,SUM(palw.earnings_amt)
,SUM(palw.earnings_loc_amt)
,MAX(palw.currency_nbr)
FROM {raw}.ps2_accrued_labor_wk_pre PALW
GROUP BY palw.week_dt
        ,palw.location_id
        ,palw.store_dept_nbr
        ,palw.earn_id
        ,palw.job_code
        ,palw.fullpt_flag
        ,palw.pay_freq_cd
ORDER BY palw.week_dt""")

SQL_INS_and_DUPS_CHECK = spark.sql(f"""SELECT COUNT(*) AS DUPLICATE_ROWS
  FROM (SELECT WEEK_DT,
               LOCATION_ID,
               EARN_ID,
               STORE_DEPT_NBR,
              JOB_CODE,
              FULLPT_FLAG,
              PAY_FREQ_CD,
               COUNT(*) AS CNT
          FROM {legacy}.PS2_ACCRUED_LABOR_WK
         GROUP BY WEEK_DT,
               LOCATION_ID,
               EARN_ID,
               STORE_DEPT_NBR,
              JOB_CODE,
              FULLPT_FLAG,
              PAY_FREQ_CD) T
 WHERE CNT > 1""")

