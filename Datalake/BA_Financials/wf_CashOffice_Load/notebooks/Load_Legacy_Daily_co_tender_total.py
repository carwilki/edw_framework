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

# PrevRunDate=genPrevRunDt('CAR_CO_TENDER_TOTAL',legacy,raw)
# print(PrevRunDate)

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

tenderTotalDf = spark.sql(f"""
    SELECT
      date_trunc('Day', TRT.TXN_TSTMP) AS SALES_DT,
      TRT.SITE_NBR,
      TRT.COUNTRY_CD,
      TRT.TOTAL_TYPE_CD,
      TRT.TENDER_TYPE_ID,
      SUM(TRT.TENDER_TOTAL_AMT) as TENDER_TOTAL_AMT
    FROM
      {refine}.carntz_co_store_tender_total TRT
      INNER JOIN {refine}.DW_LOAD_CONTROL DLC ON DLC.DW_LOAD_CONTROL_DT = date_Trunc('Day', current_timestamp())
      AND TRT.TXN_TSTMP = DLC.TXN_TSTMP
      AND TRT.TXN_KEY_GID = DLC.TXN_KEY_GID 
      GROUP BY SALES_DT,TRT.SITE_NBR,TRT.COUNTRY_CD,TRT.TOTAL_TYPE_CD,TRT.TENDER_TYPE_ID
""")

tenderTotalDf.createOrReplaceTempView("tenderTotalDf")
print("Record count:", tenderTotalDf.count())

# COMMAND ----------

_sql = f"""
INSERT INTO {legacy}.CO_TENDER_TOTAL
SELECT SALES_DT
    , SITE_NBR
    , COUNTRY_CD
    , TOTAL_TYPE_CD
    , TENDER_TYPE_ID
    , TENDER_TOTAL_AMT
    , CURRENT_TIMESTAMP AS LOAD_TSTMP
FROM tenderTotalDf
"""

# COMMAND ----------

try:
  df = spark.sql(_sql)
  logPrevRunDt("CAR_CO_TENDER_TOTAL", "CAR_CO_TENDER_TOTAL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("CAR_CO_TENDER_TOTAL", "CAR_CO_TENDER_TOTAL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
