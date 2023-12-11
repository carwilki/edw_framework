# Databricks notebook source
#Code converted on 2023-09-08 09:28:23
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

# Processing node SQ_Shortcut_to_GL_PROJECT_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_GL_PROJECT_DETAIL_PRE = spark.sql(f"""SELECT
OBJECT_CD,
PLAN_AMT,
BUDGET_AMT,
ACTUAL_AMT,
COMMITMENT_AMT
FROM {raw}.GL_PROJECT_DETAIL_PRE""")

# COMMAND ----------

# Processing node Shortcut_to_GL_PROJECT_DETAIL_PREV_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_GL_PROJECT_DETAIL_PREV_PRE = SQ_Shortcut_to_GL_PROJECT_DETAIL_PRE.selectExpr(
	"CAST(OBJECT_CD AS STRING) as OBJECT_CD",
	"CAST(PLAN_AMT AS DECIMAL(15,2)) as PLAN_AMT",
	"CAST(BUDGET_AMT AS DECIMAL(15,2)) as BUDGET_AMT",
	"CAST(ACTUAL_AMT AS DECIMAL(15,2)) as ACTUAL_AMT",
	"CAST(COMMITMENT_AMT AS DECIMAL(15,2)) as COMMITMENT_AMT"
)
Shortcut_to_GL_PROJECT_DETAIL_PREV_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_PROJECT_DETAIL_PREV_PRE')
