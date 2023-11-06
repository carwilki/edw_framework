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

       'PS2_EMPL_EMPL_LOC_WK_PRE' AS TABLE_NAME,

       COUNT(*) AS BEGIN_ROW_CNT

  FROM {empl_protected}.raw_PS2_EMPL_EMPL_LOC_WK_PRE
""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = SQ_Shortcut_to_EMPLOYEE_PROFILE_WK \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[0],'START_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[1],'TABLE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[2],'BEGIN_ROW_CNT')

# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = spark.sql(f"""INSERT OVERWRITE  {empl_protected}.raw_PS2_EMPL_EMPL_LOC_WK_PRE
SELECT
(date_add(x.week_dt, -(date_part('DOW', x.week_dt)- 1) ))
,x.location_id
,x.employee_id
,x.earn_id
,x.store_dept_nbr
,MAX(x.job_code)         JOB_CODE
,MAX(x.fullpt_flag)      FULLPT_FLAG
,SUM(x.hours_worked)     HOURS_WORKED
,SUM(x.earnings_amt)     EARNINGS_AMT
,SUM(x.earnings_loc_amt) EARNINGS_LOC_AMT
,MAX(x.pay_freq_cd)      PAY_FREQ_CD
,MAX(x.currency_nbr)     CURRENCY_NBR
FROM
 (SELECT
   p.week_dt                                    AS WEEK_DT
  ,NVL(d.location_id, NVL(s.location_id,99999)) AS LOCATION_ID
  ,e.employee_id                                AS EMPLOYEE_ID
  ,p.earn_id                                    AS EARN_ID
  ,NVL(d.store_dept_nbr, '0')                   AS STORE_DEPT_NBR
  ,e.job_code                                   AS JOB_CODE
  ,e.fullpt_flag                                AS FULLPT_FLAG
  ,p.hours_worked                               AS HOURS_WORKED
  ,DECODE(p.currency_id,'CAD',
          p.earnings_loc_amt * NVL(c.exchange_rate_pcnt, 1),
          p.earnings_loc_amt)                   AS EARNINGS_AMT
  ,p.earnings_loc_amt                           AS EARNINGS_LOC_AMT
  ,DECODE(e.ps_comp_freq_cd, 'H', 'W', 'A', 'A', 'B') AS PAY_FREQ_CD
  ,DECODE(p.currency_id, 'CAD', 1, 0)           AS CURRENCY_NBR
  FROM
   {empl_protected}.legacy_empl_earnings_wk P
  LEFT OUTER JOIN {legacy}.currency_day C
    ON (date_add(p.week_dt, -(date_part('DOW', p.week_dt)- 1) )) =  c.day_dt
  LEFT OUTER JOIN {legacy}.ps_department D
    ON p.ps_work_dept_cd = d.ps_dept_cd
  LEFT OUTER JOIN (SELECT location_id, store_nbr, location_type_id
                   FROM   {legacy}.site_profile
                   WHERE  location_type_id IN (8,15)) S
    ON (CASE WHEN INSTR(p.work_cost_center_cd ,'-') <> 0
             THEN CAST(SUBSTR(p.work_cost_center_cd,1,4) AS INTEGER)
             ELSE CAST(SUBSTR(LTRIM((decode(p.work_cost_center_cd,'0',p.home_cost_center_cd)),'0'),1,4) AS  INTEGER)
        END) = s.store_nbr
  JOIN {empl_protected}.legacy_employee_profile_wk E
    ON (date_add(p.week_dt, -(date_part('DOW', p.week_dt)- 1) )) = e.week_dt
   AND p.employee_id = e.employee_id
   where p.work_cost_center_cd<>'0' AND p.home_cost_center_cd<>'0'
   )  X
,{legacy}.site_profile SP
WHERE x.location_id = sp.location_id
  AND sp.open_dt <> sp.add_dt
  AND sp.location_type_id = 8
  AND x.week_dt > CASE WHEN x.store_dept_nbr IN (12, 13)
                       THEN date_add(sp.hotel_open_dt,-1)
                       ELSE date_add(sp.open_dt,-1)
                  END
GROUP BY (date_add(x.week_dt, -(date_part('DOW', x.week_dt)- 1) )), x.location_id, x.employee_id, x.earn_id, x.store_dept_nbr""")

SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = spark.sql(f"""SELECT COUNT(*) AS DUPLICATE_ROWS
  FROM (SELECT WEEK_DT,
               LOCATION_ID,
               EMPLOYEE_ID,
               EARN_ID,
               STORE_DEPT_NBR,
               COUNT(*) AS CNT
          FROM {empl_protected}.raw_PS2_EMPL_EMPL_LOC_WK_PRE
         GROUP BY WEEK_DT,
                  LOCATION_ID,
                  EMPLOYEE_ID,
                  EARN_ID,
                  STORE_DEPT_NBR) T
 WHERE CNT > 1""")
