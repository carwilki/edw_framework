# Databricks notebook source
#Code converted on 2023-08-09 13:02:42
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

# Processing node SQ_Shortcut_To_GL_T077S_ACCTGRP_XREF_FLAT, type SOURCE 
# COLUMN COUNT: 5

key = "t077s"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")
    
fixed_width_data = spark.read.text(file_path)

columns = [
    expr("substring(value, 1, 4)").alias("CHART_OF_ACCTS_CD"),
    expr("substring(value, 5, 4)").alias("ACCOUNT_GRP"),
    expr("substring(value, 9, 10)").alias("FROM_GL_ACCOUNT_NBR"),
    expr("substring(value, 19, 10)").alias("TO_GL_ACCOUNT_NBR"),
    expr("substring(value, 29, 30)").alias("ACCT_GRP_TYPE_CD")
]   

SQ_Shortcut_To_GL_T077S_ACCTGRP_XREF_FLAT = fixed_width_data.select(*columns)


Shortcut_To_GL_T077S_PRE = SQ_Shortcut_To_GL_T077S_ACCTGRP_XREF_FLAT.selectExpr(
	"CAST(CHART_OF_ACCTS_CD AS STRING) as CHART_OF_ACCTS_CD",
	"CAST(ACCOUNT_GRP AS STRING) as ACCT_GRP_CD",
	"CAST(FROM_GL_ACCOUNT_NBR AS INT) as FROM_GL_NBR",
	"CAST(TO_GL_ACCOUNT_NBR AS INT) as TO_GL_NBR",
	"CAST(ACCT_GRP_TYPE_CD AS STRING) as ACCT_GRP_TYPE_CD",
	"CAST((current_date() - INTERVAL 1 DAY) AS TIMESTAMP) as LOAD_DT"
)
Shortcut_To_GL_T077S_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_T077S_PRE')

# COMMAND ----------


