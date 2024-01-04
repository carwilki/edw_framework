# Databricks notebook source
#Code converted on 2023-10-11 11:42:06
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

# Processing node Shortcut_to_POG_SKU_STORE_PRO, type TARGET 
# COLUMN COUNT: 13

_target = f'{legacy}.POG_SKU_STORE_PRO'
empty_df = spark.read.table(_target).filter("false")

empty_df.write.mode("overwrite").saveAsTable(_target)

# COMMAND ----------


