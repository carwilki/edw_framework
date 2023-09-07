# Databricks notebook source
#Code converted on 2023-08-09 13:02:49
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

# Processing node SQ_Shortcut_To_GL_T023_GL_SAP_CAT_XREF_FLAT, type SOURCE 
# COLUMN COUNT: 5

key = "t023"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")
    
fixed_width_data = spark.read.text(file_path)

columns = [
    expr("substring(value, 1, 9)").alias("Mdse_catgry"),
    expr("substring(value, 10, 2)").alias("Division_ID"),
    expr("substring(value, 12, 4)").alias("Merch_Dept_id"),
    expr("substring(value, 16, 1)").alias("Active_Ind"),
    expr("substring(value, 23, 20)").alias("Met_cat_descr")
]    

SQ_Shortcut_To_GL_T023_GL_SAP_CAT_XREF_FLAT = fixed_width_data.select(*columns)

# COMMAND ----------

# Processing node FILTRANS, type FILTER 
# COLUMN COUNT: 5

FILTRANS = SQ_Shortcut_To_GL_T023_GL_SAP_CAT_XREF_FLAT.filter(col("Mdse_catgry").cast("int").isNotNull())

Shortcut_To_GL_T023_PRE = FILTRANS.selectExpr(
	"CAST(Mdse_catgry AS INT) as SAP_CATEGORY_ID",
	"IF (Division_ID IS NULL, '  ', Division_ID) as GL_DIVISION_ID",
	"IF (Merch_Dept_id IS NULL, '    ', Merch_Dept_id) as GL_CATEGORY_CD",
	"CAST(Active_Ind AS STRING) as ACTIVE_IND",
	"CAST(Met_cat_descr AS STRING) as MERCH_CAT_DESC",
	"CAST(current_date() AS TIMESTAMP) as LOAD_DT"
)
Shortcut_To_GL_T023_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_T023_PRE')


# COMMAND ----------


