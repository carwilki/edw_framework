# Databricks notebook source
#Code converted on 2023-10-13 12:47:09
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

# Processing node SQ_Shortcut_to_CO_VARIANCE, type SOURCE 
# COLUMN COUNT: 5


_sql = f"""
MERGE INTO {legacy}.LP_TENDER_VAR_DAY t 
USING (
SELECT
    SALES_DT AS DAY_DT,
    LOCATION_ID,
    TENDER_TYPE_ID,
    CASHIER_NBR,
    VARIANCE_AMT AS TENDER_VARIANCE_AMT
FROM {legacy}.SITE_PROFILE, {legacy}.CO_VARIANCE
WHERE SITE_NBR=STORE_NBR
) s
ON  t.DAY_DT          = s.DAY_DT         AND
    t.LOCATION_ID     = s.LOCATION_ID    AND
    t.TENDER_TYPE_ID  = s.TENDER_TYPE_ID AND 
    t.CASHIER_NBR     = s.CASHIER_NBR
WHEN MATCHED THEN UPDATE
    SET t.TENDER_VARIANCE_AMT = s.TENDER_VARIANCE_AMT
WHEN NOT MATCHED THEN INSERT(DAY_DT, LOCATION_ID, TENDER_TYPE_ID, CASHIER_NBR, TENDER_VARIANCE_AMT)
    VALUES(s.DAY_DT, s.LOCATION_ID, s.TENDER_TYPE_ID, s.CASHIER_NBR, s.TENDER_VARIANCE_AMT)
"""

try:
  df = spark.sql(_sql)
  logPrevRunDt("LP_TENDER_VAR_DAY", "LP_TENDER_VAR_DAY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("LP_TENDER_VAR_DAY", "LP_TENDER_VAR_DAY","Failed",str(e), f"{raw}.log_run_details", )
  raise e



# COMMAND ----------


