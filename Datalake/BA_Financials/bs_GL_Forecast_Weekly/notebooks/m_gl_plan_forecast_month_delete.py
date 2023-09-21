# Databricks notebook source
#Code converted on 2023-08-17 15:36:56
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

#Processing Pre-SQL for node Shortcut_To_GL_PLAN_FORECAST_MONTH
spark.sql(f'''DELETE FROM {legacy}.GL_PLAN_FORECAST_MONTH WHERE SUBSTR(FISCAL_MO,1,4) IN (SELECT FISCAL_YR FROM {legacy}.DAYS WHERE DAY_DT = CURRENT_DATE)''')
