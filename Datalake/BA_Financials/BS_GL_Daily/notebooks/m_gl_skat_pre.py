# Databricks notebook source
#Code converted on 2023-08-09 13:02:44
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

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")



if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# COMMAND ----------

# Processing node SQ_Shortcut_To_GL_SKAT_CHART_OF_ACCTS_FLAT, type SOURCE 
# COLUMN COUNT: 3

key = "skat"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")
    
    
fixed_width_data = spark.read.text(file_path)

columns = [
    expr("substring(value, 2, 4)").alias("CHART_OF_ACCTS_CD"),
    expr("substring(value, 6, 10)").alias("GL_ACCT_NBR"),
    expr("substring(value, 16, 50)").alias("DESC")
]

Shortcut_To_GL_SKAT_PRE = fixed_width_data.select(*columns)

Shortcut_To_GL_SKAT_PRE = Shortcut_To_GL_SKAT_PRE.selectExpr(
	"CAST(CHART_OF_ACCTS_CD AS STRING) as CHART_OF_ACCTS_CD",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(DESC AS STRING) as DESC_FLD",
	"CAST(current_date() AS TIMESTAMP) as LOAD_DT"
)
Shortcut_To_GL_SKAT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_SKAT_PRE')

# COMMAND ----------


