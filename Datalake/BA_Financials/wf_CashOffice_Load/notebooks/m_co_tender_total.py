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

# Processing node SQ_Shortcut_to_CO_TENDER_TOTAL_PRE, type SOURCE 
# COLUMN COUNT: 6

_sql = f"""
MERGE INTO {legacy}.CO_TENDER_TOTAL t 
USING {raw}.CO_TENDER_TOTAL_PRE s
ON  t.SALES_DT          = s.SALES_DT         AND
    t.SITE_NBR          = s.SITE_NBR         AND
    t.COUNTRY_CD        = s.COUNTRY_CD       AND
    t.TOTAL_TYPE_CD    = s.TOTAL_TYPE_CD   AND
    t.TENDER_TYPE_ID    = s.TENDER_TYPE_ID
WHEN MATCHED THEN UPDATE
    SET t.SALES_DT = s.SALES_DT, t.SITE_NBR = s.SITE_NBR, t.COUNTRY_CD = s.COUNTRY_CD, t.TOTAL_TYPE_CD = s.TOTAL_TYPE_CD, t.TENDER_TYPE_ID = s.TENDER_TYPE_ID, t.TENDER_TOTAL_AMT = s.TENDER_TOTAL_AMT
WHEN NOT MATCHED THEN INSERT(SALES_DT, SITE_NBR, COUNTRY_CD, TOTAL_TYPE_CD, TENDER_TYPE_ID, TENDER_TOTAL_AMT, LOAD_TSTMP)
    VALUES(s.SALES_DT, s.SITE_NBR, s.COUNTRY_CD, s.TOTAL_TYPE_CD, s.TENDER_TYPE_ID, s.TENDER_TOTAL_AMT, CURRENT_TIMESTAMP)
"""

try:
  df = spark.sql(_sql)
  logPrevRunDt("CO_TENDER_TOTAL", "CO_TENDER_TOTAL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("CO_TENDER_TOTAL", "CO_TENDER_TOTAL","Failed",str(e), f"{raw}.log_run_details", )
  raise e

