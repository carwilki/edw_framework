# Databricks notebook source
#Code converted on 2023-08-09 13:02:53
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

# Processing node SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT, type SOURCE 
# COLUMN COUNT: 2

key = "t003"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")

fixed_width_data = spark.read.text(file_path)

columns = [
    expr("rtrim(substring(value, 1, 2))").alias("DOCUMENT_TYPE_CD"),
    expr("rtrim(substring(value, 3, 20))").alias("DESC")
]

SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT = fixed_width_data.select(*columns)

# COMMAND ----------

# Processing node Shortcut_To_GL_DOC_TYPE, type TARGET 
# COLUMN COUNT: 2


Shortcut_To_GL_DOC_TYPE = SQ_Shortcut_To_GL_T003_DOC_TYPE_FLAT.selectExpr(
	"CAST(DOCUMENT_TYPE_CD AS STRING) as GL_DOC_TYPE_CD",
	"CAST(DESC AS STRING) as GL_DOC_TYPE_DESC"
)


# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.GL_DOC_TYPE"
  Shortcut_To_GL_DOC_TYPE.createOrReplaceTempView('temp_GL_DOC_TYPE')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_GL_DOC_TYPE as source
                  ON source.GL_DOC_TYPE_CD = target.GL_DOC_TYPE_CD
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e
