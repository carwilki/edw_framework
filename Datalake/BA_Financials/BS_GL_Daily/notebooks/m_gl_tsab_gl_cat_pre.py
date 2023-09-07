# Databricks notebook source
#Code converted on 2023-08-09 13:02:46
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

# Processing node SQ_Shortcut_To_GL_TSAB_GL_CAT_FLAT, type SOURCE 
# COLUMN COUNT: 2

key = "tsab"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")
    
fixed_width_data = spark.read.text(file_path)

columns = [
    expr("substring(value, 1, 4)").alias("SAP_CATEGORY_ID"),
    expr("substring(value, 5, 30)").alias("Description")
]   

SQ_Shortcut_To_GL_TSAB_GL_CAT_FLAT = fixed_width_data.select(*columns)

Shortcut_To_GL_TSAB_GL_CAT_PRE = SQ_Shortcut_To_GL_TSAB_GL_CAT_FLAT.selectExpr(
	"SUBSTR(SAP_CATEGORY_ID, 3, 2) as GL_CATEGORY_CD",
	"CAST(Description AS STRING) as DESCRIPTION",
	"CAST(CURRENT_DATE AS STRING) as LOAD_DT"
)
Shortcut_To_GL_TSAB_GL_CAT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_TSAB_GL_CAT_PRE')

# COMMAND ----------


