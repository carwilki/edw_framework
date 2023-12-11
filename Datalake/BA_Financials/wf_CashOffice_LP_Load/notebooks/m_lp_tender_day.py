# Databricks notebook source
#Code converted on 2023-10-13 12:47:08
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

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node ASQ_Shortcut_To_CASH_OFFICE_TOTAL, type SOURCE 
# COLUMN COUNT: 5

_sql = f"""
MERGE INTO {legacy}.LP_TENDER_DAY t 
USING (
SELECT C.SALES_DT DAY_DT,
       L.LOCATION_ID,
       C.TENDER_TYPE_ID, 
       MAX(CASE WHEN C.TOTAL_TYPE_CD='S' THEN C.TENDER_TOTAL_AMT ELSE 0 END) AS SYS_TENDER_AMT ,
       MAX(CASE WHEN C.TOTAL_TYPE_CD='A'THEN C.TENDER_TOTAL_AMT ELSE 0 END) AS ASSOC_TENDER_AMT
FROM {legacy}.CO_TENDER_TOTAL C, {legacy}.SITE_PROFILE L
WHERE C.SITE_NBR = L.STORE_NBR
GROUP BY C.SALES_DT, L.LOCATION_ID, C.TENDER_TYPE_ID
) s
ON  t.DAY_DT          = s.DAY_DT       AND
    t.LOCATION_ID     = s.LOCATION_ID  AND
    t.TENDER_TYPE_ID  = s.TENDER_TYPE_ID
WHEN MATCHED THEN UPDATE
    SET t.SYS_TENDER_AMT    = s.SYS_TENDER_AMT,
        t.ASSOC_TENDER_AMT  = s.ASSOC_TENDER_AMT
WHEN NOT MATCHED THEN INSERT(DAY_DT, LOCATION_ID, TENDER_TYPE_ID, SYS_TENDER_AMT, ASSOC_TENDER_AMT)
    VALUES(s.DAY_DT, s.LOCATION_ID, s.TENDER_TYPE_ID, s.SYS_TENDER_AMT, s.ASSOC_TENDER_AMT)
"""

try:
  df = spark.sql(_sql)
  logPrevRunDt("LP_TENDER_DAY", "LP_TENDER_DAY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("LP_TENDER_DAY", "LP_TENDER_DAY","Failed",str(e), f"{raw}.log_run_details", )
  raise e


# COMMAND ----------


