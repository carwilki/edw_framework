# Databricks notebook source
# Code converted on 2023-08-24 09:26:50
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

# Processing node SQ_Shortcut_to_SLOT_STATUS_CODES_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SLOT_STATUS_CODES_PRE = spark.sql(f"""select p.SL_STATUS_CD, p.SL_STATUS_DESC,

       current_date update_dt,

       nvl(t.LOAD_DT,current_date) load_dt,

       CASE WHEN t.SL_STATUS_CD IS NULL THEN 'I' ELSE 'U' END UPD_FLAG

  from {raw}.slot_status_codes_pre p

LEFT OUTER JOIN {legacy}.slot_status_codes t

  ON p.SL_STATUS_CD = t.SL_STATUS_CD""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_STATUS_CODES_PRE = SQ_Shortcut_to_SLOT_STATUS_CODES_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.columns[0],'SL_STATUS_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.columns[1],'SL_STATUS_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.columns[2],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.columns[3],'LOAD_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.columns[4],'UPD_FLAG')

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SLOT_STATUS_CODES_PRE_temp = SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.toDF(*["SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___" + col for col in SQ_Shortcut_to_SLOT_STATUS_CODES_PRE.columns])

UPDTRANS = SQ_Shortcut_to_SLOT_STATUS_CODES_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___SL_STATUS_CD as SL_STATUS_CD",
	"SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___SL_STATUS_DESC as SL_STATUS_DESC",
	"SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___LOAD_DT as LOAD_DT",
	"SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___UPD_FLAG as UPD_FLAG",
 	"IF(SQ_Shortcut_to_SLOT_STATUS_CODES_PRE___UPD_FLAG == 'I',0,1) as pyspark_data_action")

# COMMAND ----------

# Processing node Shortcut_to_SLOT_STATUS_CODES1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_SLOT_STATUS_CODES1 = UPDTRANS.selectExpr(
	"CAST(SL_STATUS_CD AS STRING) as SL_STATUS_CD",
	"CAST(SL_STATUS_DESC AS STRING) as SL_STATUS_DESC",
	"CAST(UPDATE_DT AS DATE) as UPDATE_DT",
	"CAST(LOAD_DT AS DATE) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.SL_STATUS_CD = target.SL_STATUS_CD"""
	refined_perf_table = f"{legacy}.SLOT_STATUS_CODES"
	executeMerge(Shortcut_to_SLOT_STATUS_CODES1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("SLOT_STATUS_CODES", "SLOT_STATUS_CODES", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("SLOT_STATUS_CODES", "SLOT_STATUS_CODES","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


