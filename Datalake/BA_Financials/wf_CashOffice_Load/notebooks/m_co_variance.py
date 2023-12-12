# Databricks notebook source
#Code converted on 2023-10-13 12:47:12
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

# Processing node SQ_Shortcut_to_CO_VARIANCE_PRE, type SOURCE 
# COLUMN COUNT: 7

_sql = f"""
MERGE INTO {legacy}.CO_VARIANCE t 
USING {raw}.CO_VARIANCE_PRE s
ON  t.SALES_DT          = s.SALES_DT         AND
    t.SITE_NBR          = s.SITE_NBR         AND
    t.COUNTRY_CD        = s.COUNTRY_CD       AND
    t.VARIANCE_TYPE_CD  = s.VARIANCE_TYPE_CD AND
    t.TENDER_TYPE_ID    = s.TENDER_TYPE_ID   AND
    t.CASHIER_NBR       = s.CASHIER_NBR 
WHEN MATCHED THEN UPDATE
    SET t.VARIANCE_AMT = s.VARIANCE_AMT
WHEN NOT MATCHED THEN INSERT(SALES_DT, SITE_NBR, COUNTRY_CD, VARIANCE_TYPE_CD, TENDER_TYPE_ID, CASHIER_NBR, VARIANCE_AMT, LOAD_TSTMP)
    VALUES(s.SALES_DT, s.SITE_NBR, s.COUNTRY_CD, s.VARIANCE_TYPE_CD, s.TENDER_TYPE_ID, s.CASHIER_NBR, s.VARIANCE_AMT, CURRENT_TIMESTAMP)
"""

try:
  df = spark.sql(_sql)
  logPrevRunDt("CO_VARIANCE", "CO_VARIANCE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("CO_VARIANCE", "CO_VARIANCE","Failed",str(e), f"{raw}.log_run_details", )
  raise e

# COMMAND ----------


