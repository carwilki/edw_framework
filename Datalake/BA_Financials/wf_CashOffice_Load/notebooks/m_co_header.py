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

# Processing node SQ_Shortcut_to_CO_HEADER_PRE, type SOURCE 
# COLUMN COUNT: 4

_sql = f"""
MERGE INTO {legacy}.CO_HEADER t 
USING {raw}.CO_HEADER_PRE s
ON  t.SALES_DT          = s.SALES_DT  AND
    t.SITE_NBR          = s.SITE_NBR  
WHEN MATCHED THEN UPDATE
    SET t.SOFTWARE_VERSION    = s.SOFTWARE_VERSION,
        t.FOREIGN_EXCHANGE_RT = s.FOREIGN_EXCHANGE_RT
WHEN NOT MATCHED THEN INSERT(SALES_DT, SITE_NBR, SOFTWARE_VERSION, FOREIGN_EXCHANGE_RT, LOAD_TSTMP)
    VALUES(s.SALES_DT, s.SITE_NBR, s.SOFTWARE_VERSION, s.FOREIGN_EXCHANGE_RT, CURRENT_TIMESTAMP)
"""

spark.sql(_sql)

# COMMAND ----------


