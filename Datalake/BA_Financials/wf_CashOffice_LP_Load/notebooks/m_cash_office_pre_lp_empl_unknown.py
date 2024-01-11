# Databricks notebook source
#Code converted on 2023-10-13 12:47:08
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
empl_protected = getEnvPrefix(env) + 'empl_protected'

# COMMAND ----------

# Processing node ASQ_Shortcut_to_CASH_OFFICE_VARIANCE, type SOURCE 
# COLUMN COUNT: 6

_sql = f"""
SELECT
  CASHIER_NBR,
  MIN(L.LOCATION_ID) AS LOCATION_ID,
  'Unknown' AS EMPL_FIRST_NAME,
  'CASH_OFFICE' AS EMPL_LAST_NAME,
  CURRENT_TIMESTAMP AS LOAD_DT,
  U.EMPLOYEE_ID as UNKN_EMPL
FROM {legacy}.CO_VARIANCE C LEFT JOIN {empl_protected}.legacy_EMPLOYEE_PROFILE E ON C.CASHIER_NBR = E.EMPLOYEE_ID
                   LEFT JOIN {legacy}.LP_EMPL_UNKNOWN U  ON C.CASHIER_NBR = U.EMPLOYEE_ID
                        JOIN {legacy}.SITE_PROFILE L     ON C.SITE_NBR = L.STORE_NBR
WHERE L.LOCATION_TYPE_ID = 8
  AND E.EMPLOYEE_ID IS NULL
GROUP BY CASHIER_NBR, U.EMPLOYEE_ID
"""


ASQ_Shortcut_to_CASH_OFFICE_VARIANCE = spark.sql(_sql)


# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

UPDTRANS = ASQ_Shortcut_to_CASH_OFFICE_VARIANCE.withColumn('pyspark_data_action', when(col("UNKN_EMPL").isNull(), lit(0)).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_LP_EMPL_UNKNOWN, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_LP_EMPL_UNKNOWN = UPDTRANS.selectExpr(
	"CAST(CASHIER_NBR AS BIGINT) as EMPLOYEE_ID",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(EMPL_FIRST_NAME AS STRING) as EMPL_FIRST_NAME",
	"CAST(EMPL_LAST_NAME AS STRING) as EMPL_LAST_NAME",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.EMPLOYEE_ID = target.EMPLOYEE_ID"""
	refined_perf_table = f"{legacy}.LP_EMPL_UNKNOWN"
	executeMerge(Shortcut_to_LP_EMPL_UNKNOWN, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("LP_EMPL_UNKNOWN", "LP_EMPL_UNKNOWN", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("LP_EMPL_UNKNOWN", "LP_EMPL_UNKNOWN","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		
