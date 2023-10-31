# Databricks notebook source
# Code converted on 2023-09-06 09:50:21
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
# COLUMN COUNT: 12

SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = spark.sql(f"""SELECT

 e.week_dt

,e.location_id

,e.employee_id

,e.ps_tax_company_cd || '1100'                          AS EARN_ID

,e.store_dept_nbr

,NVL(e.job_code,0)                                      AS JOB_CODE

,e.fullpt_flag

,e.ps_comp_freq_cd                                      AS PAY_FREQ_CD

,40                                                     AS HOURS_WORKED

,ROUND((40 * e.hourly_rate_loc_amt * exch_rate_pct), 2) AS EARNINGS_AMT

,ROUND((40 * e.hourly_rate_loc_amt), 2)                 AS EARNINGS_LOC_AMT

,DECODE(e.currency_id, 'CAD', 1, 0)                     AS CURRENCY_NBR

FROM

 (SELECT

   p.week_dt

  ,p.location_id

  ,p.employee_id

  ,p.ps_tax_company_cd

  ,a.pay_period_parameter

/* co-managers assigned to non-store depts need to be put back into core */
  ,CASE WHEN p.store_dept_nbr = '0' AND p.job_code IN (1604,1641,7000)

        THEN '2'

        WHEN p.store_dept_nbr = '0' AND p.job_code IN (2610)

        THEN '12'

        ELSE p.store_dept_nbr

   END AS STORE_DEPT_NBR

  ,p.job_code

  ,p.fullpt_flag

  ,p.ps_comp_freq_cd

  ,p.hourly_rate_loc_amt

  ,p.exch_rate_pct

  ,p.currency_id

  ,p.empl_status_cd

  FROM

  {empl_protected}.legacy_employee_profile_wk P
  
  JOIN {legacy}.ps_payroll_area A

    ON p.ps_payroll_area_cd = a.ps_payroll_area_cd ) E

LEFT OUTER JOIN {legacy}.ps_payroll_calendar PROLL

  ON e.ps_tax_company_cd    = proll.ps_tax_company_cd

 AND e.pay_period_parameter = proll.pay_period_parameter

 AND e.week_dt = (date_add(proll.check_dt, -(date_part('DOW', proll.check_dt)- 1) ))

LEFT OUTER JOIN {empl_protected}.raw_ps2_empl_empl_loc_wk_pre EARN

  ON e.week_dt     = earn.week_dt

 AND e.employee_id = earn.employee_id

WHERE proll.check_dt IS NULL

  AND earn.week_dt IS NULL

  AND e.empl_status_cd = 'A'

  AND e.ps_comp_freq_cd <> 'W'

  AND e.week_dt < current_date()

  AND e.ps_tax_company_cd IS NOT NULL""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EMPLOYEE_PROFILE_WK = SQ_Shortcut_to_EMPLOYEE_PROFILE_WK \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[0],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[2],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[3],'EARN_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[4],'PS_DEPT_CD') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[5],'JOB_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[6],'FULLPT_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[7],'PS_COMP_FREQ_CD') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[8],'HOURS_WORKED') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[9],'EARNINGS_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[10],'EARNINGS_LOC_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.columns[11],'CURRENCY_NBR')

# COMMAND ----------

# Processing node Shortcut_to_PS2_ACCRUED_LABOR_WK_PRE, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_PS2_ACCRUED_LABOR_WK_PRE = SQ_Shortcut_to_EMPLOYEE_PROFILE_WK.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(EMPLOYEE_ID AS INT) as EMPLOYEE_ID",
	"CAST(EARN_ID AS STRING) as EARN_ID",
	"CAST(PS_DEPT_CD AS STRING) as STORE_DEPT_NBR",
	"CAST(JOB_CODE AS INT) as JOB_CODE",
	"CAST(FULLPT_FLAG AS STRING) as FULLPT_FLAG",
	"CAST(PS_COMP_FREQ_CD AS STRING) as PAY_FREQ_CD",
	"CAST(HOURS_WORKED AS DECIMAL(11,2)) as HOURS_WORKED",
	"CAST(EARNINGS_AMT AS DECIMAL(11,2)) as EARNINGS_AMT",
	"CAST(EARNINGS_LOC_AMT AS DECIMAL(11,2)) as EARNINGS_LOC_AMT",
	"CAST(CURRENCY_NBR AS SMALLINT) as CURRENCY_NBR"
)
Shortcut_to_PS2_ACCRUED_LABOR_WK_PRE.write.mode("overwrite").saveAsTable(f'{raw}.PS2_ACCRUED_LABOR_WK_PRE')





