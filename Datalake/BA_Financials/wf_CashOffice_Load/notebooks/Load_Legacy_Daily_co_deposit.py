# Databricks notebook source
from Datalake.utils.genericUtilities import *
from pyspark.sql.types import *
from  pyspark.sql.functions import expr
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + "raw"
refine = getEnvPrefix(env) + 'refine'
legacy = getEnvPrefix(env) + 'legacy'

# PrevRunDate=genPrevRunDt('CAR_CO_DEPOSIT',legacy,raw)
# print(PrevRunDate)

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

depositDf = spark.sql(f"""
    SELECT
      date_trunc('Day', TRT.TXN_TSTMP) AS SALES_DT,
      TRT.SITE_NBR,
      TRT.COUNTRY_CD,
      TRT.DEPOSIT_TYPE_CD,
      "" as GL_ACCT,
      TRT.TENDER_TYPE_ID,
      coalesce(TRT.DEPOSIT_SLIP_NBR,0) as DEPOSIT_SLIP_NBR,
      TRT.DEPOSIT_BAG_NBR,
      TRT.CO_SEQ_NBR as SEQ_NBR,
      TRT.DEPOSIT_AMT
    FROM
      {refine}.carntz_co_deposit TRT
     INNER JOIN {refine}.DW_LOAD_CONTROL DLC ON DLC.DW_LOAD_CONTROL_DT = date_Trunc('Day', current_timestamp())
     AND TRT.TXN_TSTMP = DLC.TXN_TSTMP
     AND TRT.TXN_KEY_GID = DLC.TXN_KEY_GID 
""")

depositDf.createOrReplaceTempView("view_depositDf")
print("Record count:", depositDf.count())

# COMMAND ----------

_sql = f"""
INSERT INTO {legacy}.CO_DEPOSIT
SELECT SALES_DT
    , SITE_NBR
    , COUNTRY_CD
    , DEPOSIT_TYPE_CD
    , GL_ACCT
    , TENDER_TYPE_ID
    , DEPOSIT_SLIP_NBR
    , DEPOSIT_BAG_NBR
    , SEQ_NBR
    , DEPOSIT_AMT
    , CURRENT_TIMESTAMP AS LOAD_TSTMP
FROM view_depositDf
"""

# COMMAND ----------

try:
  df = spark.sql(_sql)
  logPrevRunDt("CAR_CO_DEPOSIT", "CAR_CO_DEPOSIT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("CAR_CO_DEPOSIT", "CAR_CO_DEPOSIT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
