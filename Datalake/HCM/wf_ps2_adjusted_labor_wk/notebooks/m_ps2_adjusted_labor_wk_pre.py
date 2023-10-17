# Databricks notebook source
# Code converted on 2023-09-06 09:51:45
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

from datetime import datetime, timezone
dt=datetime.now(timezone.utc)
hr=dt.hour
hr=21
print('The hour is' + str(hr))
if not (hr >= 7 and hr<=20):
    dbutils.jobs.taskValues.set(key = "isvalidhour", value = 'true')
    dbutils.notebook.exit('The UTC Hour is not in the range 7 AM and 20 PM, so the execution is stopped')
else:
    print('The decision allows to proceed further with execution because the Hour is within the expected range')

# COMMAND ----------

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/employee/ps2adjlbr/')
source_bucket = getParameterValue(
    raw,
    "BA_HCM.prm",
    "BA_HCM.WF:wf_ps2_adjusted_labor_wk",
    "source_bucket",
)

def get_source_file(_bucket):
  import builtins

  lst = dbutils.fs.ls(_bucket)
  fldr = builtins.max(lst, key=lambda x: x.name).name
  lst = dbutils.fs.ls(_bucket + fldr)
  files = [x.path for x in lst]
  return files[0] if files else None


# COMMAND ----------

# Processing node SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT, type SOURCE 
# COLUMN COUNT: 2

file_path = get_source_file(source_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")

SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT = spark.read.options(header='True',delimiter=',').csv(file_path)


# COMMAND ----------

# Processing node Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE, type TARGET 
# COLUMN COUNT: 11
SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT = SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT.select([col(c).alias(c.replace(' ', '_')) for c in SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT.columns])

Shortcut_to_PS2_ADJUSTED_LABOR_WK_cast=SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT.selectExpr(
	"CAST(to_date(WeekDt,'MM/dd/yyyy') AS TIMESTAMP) as WEEK_DT",
	"CAST(Store AS INT) as STORE_NBR",
	"CAST(Earnings_ID AS STRING) as EARN_ID",
	"CAST(Dept_Nbr AS SMALLINT) as STORE_DEPT_NBR",
	"CAST(Job_Code AS INT) as JOB_CODE",
	"CAST(Act_Hours_Worked AS DECIMAL(9,2)) as ACT_HOURS_WORKED",
	"CAST(Earnings_Loc AS DECIMAL(9,4)) as ACT_EARNINGS_LOC_AMT",
	"CAST(Earned_Hours AS DECIMAL(9,2)) as EARNED_HOURS",
	"CAST(Earned_Amt AS DECIMAL(15,4)) as EARNED_LOC_AMT",
	"CAST(Forecast_Hours AS DECIMAL(9,2)) as FORECAST_HRS",
	"CAST(Forecast_Amt_Loc AS DECIMAL(9,4)) as FORECAST_LOC_AMT"
)

Shortcut_to_PS2_ADJUSTED_LABOR_WK_agg=Shortcut_to_PS2_ADJUSTED_LABOR_WK_cast.groupBy("WEEK_DT","STORE_NBR","EARN_ID","STORE_DEPT_NBR","JOB_CODE").agg(
  sum('ACT_HOURS_WORKED').alias("ACT_HOURS_WORKED"),
  sum('ACT_EARNINGS_LOC_AMT').alias('ACT_EARNINGS_LOC_AMT'),
  sum('EARNED_HOURS').alias('EARNED_HOURS'),
  sum('EARNED_LOC_AMT').alias('EARNED_LOC_AMT'),
  sum('FORECAST_HRS').alias('FORECAST_HRS'),
  sum('FORECAST_LOC_AMT').alias('FORECAST_LOC_AMT')
).selectExpr(
  "WEEK_DT",
  "STORE_NBR",
  "EARN_ID",
  "STORE_DEPT_NBR",
  "JOB_CODE",
  "CAST(ACT_HOURS_WORKED AS DECIMAL(9,2)) as ACT_HOURS_WORKED",
  "CAST(ACT_EARNINGS_LOC_AMT AS DECIMAL(9,4)) as ACT_EARNINGS_LOC_AMT",
  "CAST(EARNED_HOURS AS DECIMAL(9,2)) as EARNED_HOURS",
  "CAST(EARNED_LOC_AMT AS DECIMAL(15,4)) as EARNED_LOC_AMT",
  "CAST(FORECAST_HRS AS DECIMAL(9,2)) as FORECAST_HRS",
  "CAST(FORECAST_LOC_AMT AS DECIMAL(9,4)) as FORECAST_LOC_AMT"
)
 

Shortcut_to_PS2_ADJUSTED_LABOR_WK_agg.write.mode("overwrite").saveAsTable(f'{raw}.PS2_ADJUSTED_LABOR_WK_PRE')

# COMMAND ----------

count=spark.table(f'{raw}.PS2_ADJUSTED_LABOR_WK_PRE').count()
print('The count of the pre table is ' + str(count))
if (count== 0):
    dbutils.jobs.taskValues.set(key = "tgtsuccessrows", value = 'true')
