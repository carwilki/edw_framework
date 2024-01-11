# Databricks notebook source
#Code converted on 2023-10-13 12:47:11
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

# Processing node SQ_Shortcut_to_CO_DEPOSIT_PRE, type SOURCE 
# COLUMN COUNT: 10

_sql = f"""
MERGE INTO {legacy}.CO_DEPOSIT t 
USING {raw}.CO_DEPOSIT_PRE s
ON  t.SALES_DT          = s.SALES_DT         AND
    t.SITE_NBR          = s.SITE_NBR         AND
    t.COUNTRY_CD        = s.COUNTRY_CD       AND
    t.DEPOSIT_TYPE_CD   = s.DEPOSIT_TYPE_CD  AND
    t.GL_ACCT           = s.GL_ACCT          AND
    t.TENDER_TYPE_ID    = s.TENDER_TYPE_ID   AND
    t.DEPOSIT_SLIP_NBR  = s.DEPOSIT_SLIP_NBR AND
    t.DEPOSIT_BAG_NBR   = s.DEPOSIT_BAG_NBR  AND
    t.SEQ_NBR           = s.SEQ_NBR        
WHEN MATCHED THEN UPDATE
    SET t.DEPOSIT_AMT = s.DEPOSIT_AMT
WHEN NOT MATCHED THEN INSERT(SALES_DT, SITE_NBR, COUNTRY_CD, DEPOSIT_TYPE_CD, GL_ACCT, TENDER_TYPE_ID, DEPOSIT_SLIP_NBR, DEPOSIT_BAG_NBR, SEQ_NBR, DEPOSIT_AMT, LOAD_TSTMP)
    VALUES(s.SALES_DT, s.SITE_NBR, s.COUNTRY_CD, s.DEPOSIT_TYPE_CD, s.GL_ACCT, s.TENDER_TYPE_ID, s.DEPOSIT_SLIP_NBR, s.DEPOSIT_BAG_NBR, s.SEQ_NBR, s.DEPOSIT_AMT, CURRENT_TIMESTAMP)
"""

try:
  df = spark.sql(_sql)
  logPrevRunDt("CO_DEPOSIT", "CO_DEPOSIT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("CO_DEPOSIT", "CO_DEPOSIT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
