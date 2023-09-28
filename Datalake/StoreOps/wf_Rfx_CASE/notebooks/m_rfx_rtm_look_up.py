# Databricks notebook source
# Code converted on 2023-08-25 11:50:35
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

# Processing node SQ_Shortcut_to_RFX_LOOK_UP_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_RFX_LOOK_UP_PRE = spark.sql(f"""SELECT
RFX_LOOK_UP_PRE.LOOKUP_TYPE,
RFX_LOOK_UP_PRE.RFX_KEY,
RFX_LOOK_UP_PRE.RFX_DESCRIPTION
FROM {raw}.RFX_LOOK_UP_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_RTM_LOOK_UP, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_RFX_RTM_LOOK_UP = spark.sql(f"""SELECT
RFX_RTM_LOOK_UP.RFX_LOOKUP_TYPE_CD,
RFX_RTM_LOOK_UP.RFX_KEY_CD,
RFX_RTM_LOOK_UP.RFX_KEY_DESC,
RFX_RTM_LOOK_UP.LOAD_TSTMP
FROM {legacy}.RFX_RTM_LOOK_UP""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_RFX_RTM_LOOK_UP, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_LOOK_UP_PRE_temp = SQ_Shortcut_to_RFX_LOOK_UP_PRE.toDF(*["SQ_Shortcut_to_RFX_LOOK_UP_PRE___" + col for col in SQ_Shortcut_to_RFX_LOOK_UP_PRE.columns])
SQ_Shortcut_to_RFX_RTM_LOOK_UP_temp = SQ_Shortcut_to_RFX_RTM_LOOK_UP.toDF(*["SQ_Shortcut_to_RFX_RTM_LOOK_UP___" + col for col in SQ_Shortcut_to_RFX_RTM_LOOK_UP.columns])

JNR_RFX_RTM_LOOK_UP = SQ_Shortcut_to_RFX_LOOK_UP_PRE_temp.join(SQ_Shortcut_to_RFX_RTM_LOOK_UP_temp,[SQ_Shortcut_to_RFX_LOOK_UP_PRE_temp.SQ_Shortcut_to_RFX_LOOK_UP_PRE___LOOKUP_TYPE == SQ_Shortcut_to_RFX_RTM_LOOK_UP_temp.SQ_Shortcut_to_RFX_RTM_LOOK_UP___RFX_LOOKUP_TYPE_CD, SQ_Shortcut_to_RFX_LOOK_UP_PRE_temp.SQ_Shortcut_to_RFX_LOOK_UP_PRE___RFX_KEY == SQ_Shortcut_to_RFX_RTM_LOOK_UP_temp.SQ_Shortcut_to_RFX_RTM_LOOK_UP___RFX_KEY_CD],'left_outer').selectExpr(
	"SQ_Shortcut_to_RFX_LOOK_UP_PRE___LOOKUP_TYPE as LOOKUP_TYPE",
	"SQ_Shortcut_to_RFX_LOOK_UP_PRE___RFX_KEY as RFX_KEY",
	"SQ_Shortcut_to_RFX_LOOK_UP_PRE___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"SQ_Shortcut_to_RFX_RTM_LOOK_UP___RFX_LOOKUP_TYPE_CD as i_RFX_LOOKUP_TYPE_CD",
	"SQ_Shortcut_to_RFX_RTM_LOOK_UP___RFX_KEY_CD as i_RFX_KEY_CD",
	"SQ_Shortcut_to_RFX_RTM_LOOK_UP___RFX_KEY_DESC as i_RFX_KEY_DESC",
	"SQ_Shortcut_to_RFX_RTM_LOOK_UP___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_RFX_RTM_LOOK_UP, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_RFX_RTM_LOOK_UP_temp = JNR_RFX_RTM_LOOK_UP.toDF(*["JNR_RFX_RTM_LOOK_UP___" + col for col in JNR_RFX_RTM_LOOK_UP.columns])

FIL_RFX_RTM_LOOK_UP = JNR_RFX_RTM_LOOK_UP_temp.selectExpr(
	"JNR_RFX_RTM_LOOK_UP___LOOKUP_TYPE as LOOKUP_TYPE",
	"JNR_RFX_RTM_LOOK_UP___RFX_KEY as RFX_KEY",
	"JNR_RFX_RTM_LOOK_UP___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"JNR_RFX_RTM_LOOK_UP___i_RFX_LOOKUP_TYPE_CD as i_RFX_LOOKUP_TYPE_CD",
	"JNR_RFX_RTM_LOOK_UP___i_RFX_KEY_CD as i_RFX_KEY_CD",
	"JNR_RFX_RTM_LOOK_UP___i_RFX_KEY_DESC as i_RFX_KEY_DESC",
	"JNR_RFX_RTM_LOOK_UP___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_RFX_LOOKUP_TYPE_CD IS NULL AND i_RFX_KEY_CD IS NULL OR ( i_RFX_LOOKUP_TYPE_CD IS NOT NULL AND i_RFX_KEY_CD IS NOT NULL AND ( IF (RFX_DESCRIPTION IS NULL, '', RFX_DESCRIPTION) != IF (i_RFX_KEY_DESC IS NULL, '', i_RFX_KEY_DESC) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_RFX_RTM_LOOK_UP, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
FIL_RFX_RTM_LOOK_UP_temp = FIL_RFX_RTM_LOOK_UP.toDF(*["FIL_RFX_RTM_LOOK_UP___" + col for col in FIL_RFX_RTM_LOOK_UP.columns])

EXP_RFX_RTM_LOOK_UP = FIL_RFX_RTM_LOOK_UP_temp.selectExpr(
	"FIL_RFX_RTM_LOOK_UP___sys_row_id as sys_row_id",
	"FIL_RFX_RTM_LOOK_UP___LOOKUP_TYPE as LOOKUP_TYPE",
	"FIL_RFX_RTM_LOOK_UP___RFX_KEY as RFX_KEY",
	"FIL_RFX_RTM_LOOK_UP___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"FIL_RFX_RTM_LOOK_UP___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"FIL_RFX_RTM_LOOK_UP___i_RFX_LOOKUP_TYPE_CD as i_RFX_LOOKUP_TYPE_CD",
	"FIL_RFX_RTM_LOOK_UP___i_RFX_KEY_CD as i_RFX_KEY_CD",
	"IF (( FIL_RFX_RTM_LOOK_UP___i_RFX_LOOKUP_TYPE_CD IS NULL AND FIL_RFX_RTM_LOOK_UP___i_RFX_KEY_CD IS NULL ), 1, 2) as o_UPDATE_VALIDATOR",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_RFX_RTM_LOOK_UP___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_RFX_RTM_LOOK_UP___i_LOAD_TSTMP) as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node UPD_RFX_RTM_LOOK_UP, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_RFX_RTM_LOOK_UP_temp = EXP_RFX_RTM_LOOK_UP.toDF(*["EXP_RFX_RTM_LOOK_UP___" + col for col in EXP_RFX_RTM_LOOK_UP.columns])

UPD_RFX_RTM_LOOK_UP = EXP_RFX_RTM_LOOK_UP_temp.selectExpr(
	"EXP_RFX_RTM_LOOK_UP___LOOKUP_TYPE as LOOKUP_TYPE",
	"EXP_RFX_RTM_LOOK_UP___RFX_KEY as RFX_KEY",
	"EXP_RFX_RTM_LOOK_UP___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"EXP_RFX_RTM_LOOK_UP___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
	"EXP_RFX_RTM_LOOK_UP___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_RFX_RTM_LOOK_UP___LOAD_TSTMP as LOAD_TSTMP",
	"if(EXP_RFX_RTM_LOOK_UP___o_UPDATE_VALIDATOR==1,0,1) as pyspark_data_action")

# COMMAND ----------

# Processing node Shortcut_to_RFX_RTM_LOOK_UP1, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_RFX_RTM_LOOK_UP1 = UPD_RFX_RTM_LOOK_UP.selectExpr(
	"CAST(LOOKUP_TYPE AS STRING) as RFX_LOOKUP_TYPE_CD",
	"CAST(RFX_KEY AS STRING) as RFX_KEY_CD",
	"CAST(RFX_DESCRIPTION AS STRING) as RFX_KEY_DESC",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.RFX_LOOKUP_TYPE_CD = target.RFX_LOOKUP_TYPE_CD AND source.RFX_KEY_CD = target.RFX_KEY_CD"""
	refined_perf_table = f"{legacy}.RFX_RTM_LOOK_UP"
	executeMerge(Shortcut_to_RFX_RTM_LOOK_UP1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("RFX_RTM_LOOK_UP", "RFX_RTM_LOOK_UP", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("RFX_RTM_LOOK_UP", "RFX_RTM_LOOK_UP","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


