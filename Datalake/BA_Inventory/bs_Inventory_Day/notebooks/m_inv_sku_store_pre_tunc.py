# Databricks notebook source
#Code converted on 2023-09-15 14:55:13
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

# Processing node SQ_Shortcut_To_WEEKS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_WEEKS = spark.sql(f""" select CURRENT_TIMESTAMP AS WEEK_DT
""").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node FLT_INV_SKU_STORE_PRE, type FILTER 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_WEEKS_temp = SQ_Shortcut_To_WEEKS.toDF(*["SQ_Shortcut_To_WEEKS___" + col for col in SQ_Shortcut_To_WEEKS.columns])

FLT_INV_SKU_STORE_PRE = SQ_Shortcut_To_WEEKS.filter("false")

# COMMAND ----------

# Processing node Shortcut_To_BIW_SCRIPT_DUMMY, type TARGET 
# COLUMN COUNT: 1


Shortcut_To_BIW_SCRIPT_DUMMY = SQ_Shortcut_To_WEEKS.selectExpr(
	"WEEK_DT as DATE_TIME"
)

Shortcut_To_BIW_SCRIPT_DUMMY.write.mode("overwrite").saveAsTable(f'{raw}.BIW_SCRIPT_DUMMY')

# COMMAND ----------

# Processing node Shortcut_to_INV_SKU_STORE_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_INV_SKU_STORE_PRE = FLT_INV_SKU_STORE_PRE.selectExpr(
	"0 as PRODUCT_ID",
	"0 as LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as FIRST_ON_HAND_DT",
	"CAST(NULL AS TIMESTAMP) as LAST_ON_HAND_DT",
	"CAST(NULL AS TIMESTAMP) as LOAD_DT"
)
Shortcut_to_INV_SKU_STORE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.INV_SKU_STORE_PRE')
