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

# PrevRunDate=genPrevRunDt('CAR_CO_VARIANCE',legacy,raw)
# print(PrevRunDate)

# COMMAND ----------


from pyspark.sql.types import *
from  pyspark.sql.functions import expr

spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

tenderTotalDf = spark.sql(f"""
    SELECT
      date_trunc('Day', TRT.TXN_TSTMP) AS SALES_DT,
      TRT.SITE_NBR,
      TRT.COUNTRY_CD,
      TRT.VARIANCE_TYPE_CD,
      TRT.TENDER_TYPE_ID,
      coalesce(TRT.CASHIER_NBR, trim(concat(TRT.REGISTER_NBR,TRT.REGISTER_NBR))) AS CASHIER_NBR,
      SUM(TRT.VARIANCE_AMT) as VARIANCE_AMT
    FROM
      {refine}.carntz_co_reg_tender_total TRT
       INNER JOIN {refine}.DW_LOAD_CONTROL DLC ON DLC.DW_LOAD_CONTROL_DT = date_Trunc('Day', current_timestamp())
      AND TRT.TXN_TSTMP = DLC.TXN_TSTMP
      AND TRT.TXN_KEY_GID = DLC.TXN_KEY_GID 
      GROUP BY SALES_DT,TRT.SITE_NBR,TRT.COUNTRY_CD,TRT.VARIANCE_TYPE_CD,TRT.TENDER_TYPE_ID,coalesce(TRT.CASHIER_NBR, trim(concat(TRT.REGISTER_NBR,TRT.REGISTER_NBR)))
""")

tenderTotalDf.createOrReplaceTempView("tenderTotalDf")
print("Record count:", tenderTotalDf.count())

# COMMAND ----------

_sql = f"""
INSERT INTO {legacy}.CO_VARIANCE
SELECT SALES_DT
    , SITE_NBR
    , COUNTRY_CD
    , VARIANCE_TYPE_CD
    , TENDER_TYPE_ID
    , CASHIER_NBR
    , VARIANCE_AMT
    , CURRENT_TIMESTAMP AS LOAD_TSTMP
FROM tenderTotalDf
"""

# COMMAND ----------

try:
  spark.sql(_sql)
  logPrevRunDt("CAR_CO_VARIANCE", "CAR_CO_VARIANCE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("CAR_CO_VARIANCE", "CAR_CO_VARIANCE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
