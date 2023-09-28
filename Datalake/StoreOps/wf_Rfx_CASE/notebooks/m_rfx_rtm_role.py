# Databricks notebook source
# Code converted on 2023-08-25 11:50:21
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

# Processing node SQ_Shortcut_to_RFX_RTM_ROLE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_RFX_RTM_ROLE = spark.sql(f"""SELECT
RFX_RTM_ROLE.RFX_ROLE_CD,
RFX_RTM_ROLE.RFX_ROLE_DESC,
RFX_RTM_ROLE.LOAD_TSTMP
FROM {legacy}.RFX_RTM_ROLE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_RFX_LOOKUP_PRE, type FILTER 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_LOOK_UP_PRE_temp = SQ_Shortcut_to_RFX_LOOK_UP_PRE.toDF(*["SQ_Shortcut_to_RFX_LOOK_UP_PRE___" + col for col in SQ_Shortcut_to_RFX_LOOK_UP_PRE.columns])

FIL_RFX_LOOKUP_PRE = SQ_Shortcut_to_RFX_LOOK_UP_PRE_temp.selectExpr(
	"SQ_Shortcut_to_RFX_LOOK_UP_PRE___LOOKUP_TYPE as LOOKUP_TYPE",
	"SQ_Shortcut_to_RFX_LOOK_UP_PRE___RFX_KEY as RFX_KEY",
	"SQ_Shortcut_to_RFX_LOOK_UP_PRE___RFX_DESCRIPTION as RFX_DESCRIPTION").filter("LOOKUP_TYPE = 'ROLE'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_RFX_RTM_ROLE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_RTM_ROLE_temp = SQ_Shortcut_to_RFX_RTM_ROLE.toDF(*["SQ_Shortcut_to_RFX_RTM_ROLE___" + col for col in SQ_Shortcut_to_RFX_RTM_ROLE.columns])
FIL_RFX_LOOKUP_PRE_temp = FIL_RFX_LOOKUP_PRE.toDF(*["FIL_RFX_LOOKUP_PRE___" + col for col in FIL_RFX_LOOKUP_PRE.columns])

JNR_RFX_RTM_ROLE = SQ_Shortcut_to_RFX_RTM_ROLE_temp.join(FIL_RFX_LOOKUP_PRE_temp,[SQ_Shortcut_to_RFX_RTM_ROLE_temp.SQ_Shortcut_to_RFX_RTM_ROLE___RFX_ROLE_CD == FIL_RFX_LOOKUP_PRE_temp.FIL_RFX_LOOKUP_PRE___RFX_KEY],'right_outer').selectExpr(
	"FIL_RFX_LOOKUP_PRE___RFX_KEY as RFX_KEY",
	"FIL_RFX_LOOKUP_PRE___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"SQ_Shortcut_to_RFX_RTM_ROLE___RFX_ROLE_CD as i_RFX_ROLE_CD",
	"SQ_Shortcut_to_RFX_RTM_ROLE___RFX_ROLE_DESC as i_RFX_ROLE_DESC",
	"SQ_Shortcut_to_RFX_RTM_ROLE___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_RFX_RTM_ROLE, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_RFX_RTM_ROLE_temp = JNR_RFX_RTM_ROLE.toDF(*["JNR_RFX_RTM_ROLE___" + col for col in JNR_RFX_RTM_ROLE.columns])

FIL_RFX_RTM_ROLE = JNR_RFX_RTM_ROLE_temp.selectExpr(
	"JNR_RFX_RTM_ROLE___RFX_KEY as RFX_KEY",
	"JNR_RFX_RTM_ROLE___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"JNR_RFX_RTM_ROLE___i_RFX_ROLE_CD as i_RFX_ROLE_CD",
	"JNR_RFX_RTM_ROLE___i_RFX_ROLE_DESC as i_RFX_ROLE_DESC",
	"JNR_RFX_RTM_ROLE___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_RFX_ROLE_CD IS NULL OR ( i_RFX_ROLE_CD IS NOT NULL AND IF (i_RFX_ROLE_DESC IS NULL, '', i_RFX_ROLE_DESC) != IF (RFX_DESCRIPTION IS NULL, '', RFX_DESCRIPTION) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_RFX_RTM_ROLE, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_RFX_RTM_ROLE_temp = FIL_RFX_RTM_ROLE.toDF(*["FIL_RFX_RTM_ROLE___" + col for col in FIL_RFX_RTM_ROLE.columns])

EXP_RFX_RTM_ROLE = FIL_RFX_RTM_ROLE_temp.selectExpr(
	"FIL_RFX_RTM_ROLE___sys_row_id as sys_row_id",
	"FIL_RFX_RTM_ROLE___RFX_KEY as RFX_KEY",
	"FIL_RFX_RTM_ROLE___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"FIL_RFX_RTM_ROLE___i_RFX_ROLE_CD as i_RFX_ROLE_CD",
	"FIL_RFX_RTM_ROLE___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"IF (FIL_RFX_RTM_ROLE___i_RFX_ROLE_CD IS NULL, 1, 2) as o_UPDATE_VALIDATOR",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_RFX_RTM_ROLE___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_RFX_RTM_ROLE___i_LOAD_TSTMP) as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node UPD_RFX_RTM_ROLE, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_RFX_RTM_ROLE_temp = EXP_RFX_RTM_ROLE.toDF(*["EXP_RFX_RTM_ROLE___" + col for col in EXP_RFX_RTM_ROLE.columns])

UPD_RFX_RTM_ROLE = EXP_RFX_RTM_ROLE_temp.selectExpr(
	"EXP_RFX_RTM_ROLE___RFX_KEY as RFX_KEY",
	"EXP_RFX_RTM_ROLE___RFX_DESCRIPTION as RFX_DESCRIPTION",
	"EXP_RFX_RTM_ROLE___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
	"EXP_RFX_RTM_ROLE___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_RFX_RTM_ROLE___LOAD_TSTMP as LOAD_TSTMP",
	"if(EXP_RFX_RTM_ROLE___o_UPDATE_VALIDATOR==1,0,1) as pyspark_data_action")


# COMMAND ----------

# Processing node Shortcut_to_RFX_RTM_ROLE, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_RFX_RTM_ROLE = UPD_RFX_RTM_ROLE.selectExpr(
	"CAST(RFX_KEY AS STRING) as RFX_ROLE_CD",
	"CAST(RFX_DESCRIPTION AS STRING) as RFX_ROLE_DESC",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.RFX_ROLE_CD = target.RFX_ROLE_CD"""
	refined_perf_table = f"{legacy}.RFX_RTM_ROLE"
	executeMerge(Shortcut_to_RFX_RTM_ROLE, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("RFX_RTM_ROLE", "RFX_RTM_ROLE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("RFX_RTM_ROLE", "RFX_RTM_ROLE","Failed",str(e), f"{raw}.log_run_details")
	raise e
		
