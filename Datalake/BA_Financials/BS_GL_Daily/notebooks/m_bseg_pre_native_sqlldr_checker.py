# Databricks notebook source
#Code converted on 2023-08-09 13:03:00
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
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

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")



if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# COMMAND ----------

# Processing node ASQ_Shortcut_To_DAYS, type SOURCE 
# COLUMN COUNT: 1

ASQ_Shortcut_To_DAYS = spark.table(f"{raw}.gl_bseg_pre")

count=ASQ_Shortcut_To_DAYS.count()

if count > 0:
    print("m_bseg_pre_native_sqlldr_checker count is " + str(count)) 
else:
    raise Exception("Aborting workflow , no data in the gl_bseg_pre table")  

