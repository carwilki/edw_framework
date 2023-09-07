# Databricks notebook source
#Code converted on 2023-08-09 13:02:50
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

# Processing node ASQ_Shortcut_To_GL_T023_PRE, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
SELECT c.sap_category_id, COALESCE(x.gl_category_cd, 'Z099') AS gl_category_cd
FROM {legacy}.sap_category C LEFT OUTER JOIN {raw}.gl_t023_pre X ON c.sap_category_id = x.sap_category_id
"""

ASQ_Shortcut_To_GL_T023_PRE = spark.sql(_sql)


# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 1


EXPTRANS = ASQ_Shortcut_To_GL_T023_PRE.selectExpr(
    "sap_category_id as sap_category_id",
    "IF(gl_category_cd = '    ','00',SUBSTR(gl_category_cd,3,2)) as gl_category_cd"
)

# COMMAND ----------

# Processing node Shortcut_To_SAP_CATEGORY2, type TARGET 
# COLUMN COUNT: 6

Shortcut_To_SAP_CATEGORY2 = EXPTRANS.selectExpr(
	"CAST(SAP_CATEGORY_ID AS INT) as SAP_CATEGORY_ID",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD"
)

try:
  refined_perf_table = f"{legacy}.SAP_CATEGORY"
  Shortcut_To_SAP_CATEGORY2.createOrReplaceTempView('temp_sap_cat')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_sap_cat as source
                  ON source.SAP_CATEGORY_ID = target.SAP_CATEGORY_ID
                  WHEN MATCHED THEN
                    UPDATE SET target.GL_CATEGORY_CD = source.GL_CATEGORY_CD, upd_tstmp = CURRENT_TIMESTAMP
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e
