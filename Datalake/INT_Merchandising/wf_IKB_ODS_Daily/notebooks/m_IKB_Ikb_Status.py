# Databricks notebook source
#Code converted on 2023-09-12 13:31:04
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

# Read in relation source variables
(username, password, connection_string) = ckb_prd_sqlServer(env)
db_name = "CKB_PRD"

# COMMAND ----------

# Processing node SQ_ix_sys_status, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""SELECT
ix_sys_status.DBKey,
ix_sys_status.Description
FROM {db_name}.dbo.ix_sys_status"""

SQ_ix_sys_status = jdbcSqlServerConnection(f"({_sql}) as x",username,password,connection_string)  #.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_IKB_STATUS, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_IKB_STATUS = spark.sql(f"""SELECT
IKB_STATUS_ID,
IKB_STATUS_DESC,
LOAD_TSTMP AS LOAD_DT
FROM {legacy}.IKB_STATUS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_LeftJoin1, type JOINER 
# COLUMN COUNT: 5

JNR_LeftJoin1 = SQ_Shortcut_to_IKB_STATUS.join(SQ_ix_sys_status,[SQ_Shortcut_to_IKB_STATUS.IKB_STATUS_ID == SQ_ix_sys_status.DBKey],'right_outer')

# COMMAND ----------

# Processing node EXP_CheckChanges, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_LeftJoin1_temp = JNR_LeftJoin1.toDF(*["JNR_LeftJoin1___" + col for col in JNR_LeftJoin1.columns])

EXP_CheckChanges = JNR_LeftJoin1_temp.selectExpr(
	"JNR_LeftJoin1___sys_row_id as sys_row_id",
	"JNR_LeftJoin1___DBKey as DBKey",
	"JNR_LeftJoin1___Description as Description",
	"IF (JNR_LeftJoin1___LOAD_DT IS NULL, CURRENT_TIMESTAMP, JNR_LeftJoin1___LOAD_DT) as LOAD_DT_NOTNULL",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (JNR_LeftJoin1___IKB_STATUS_ID IS NULL, 0, IF (IF (JNR_LeftJoin1___Description IS NULL, 'N\A', JNR_LeftJoin1___Description) != IF (JNR_LeftJoin1___IKB_STATUS_DESC IS NULL, 'N\A', JNR_LeftJoin1___IKB_STATUS_DESC), 1, 3)) as UPDATE_STRATEGY"
)

# COMMAND ----------

# Processing node FIL_RemoveRejected, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___DBKey as DBKey",
	"EXP_CheckChanges___Description as Description",
	"EXP_CheckChanges___LOAD_DT_NOTNULL as LOAD_DT_NOTNULL",
	"EXP_CheckChanges___UPDATE_DT as UPDATE_DT",
	"EXP_CheckChanges___UPDATE_STRATEGY as UPDATE_STRATEGY").filter("UPDATE_STRATEGY != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_SetStrategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FIL_RemoveRejected_temp = FIL_RemoveRejected.toDF(*["FIL_RemoveRejected___" + col for col in FIL_RemoveRejected.columns])

UPD_SetStrategy = FIL_RemoveRejected_temp.selectExpr(
	"FIL_RemoveRejected___DBKey as DBKey",
	"FIL_RemoveRejected___Description as Description",
	"FIL_RemoveRejected___LOAD_DT_NOTNULL as LOAD_DT_NOTNULL",
	"FIL_RemoveRejected___UPDATE_DT as UPDATE_DT",
	"FIL_RemoveRejected___UPDATE_STRATEGY as UPDATE_STRATEGY") \
	.withColumn('pyspark_data_action', col("UPDATE_STRATEGY"))

# COMMAND ----------

# Processing node Shortcut_to_IKB_STATUS_1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_IKB_STATUS_1 = UPD_SetStrategy.selectExpr(
	"CAST(DBKey AS INT) as IKB_STATUS_ID",
	"CAST(Description AS STRING) as IKB_STATUS_DESC",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_DT_NOTNULL AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.IKB_STATUS_ID = target.IKB_STATUS_ID"""
	refined_perf_table = f"{legacy}.IKB_STATUS"
	executeMerge(Shortcut_to_IKB_STATUS_1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("IKB_STATUS", "IKB_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("IKB_STATUS", "IKB_STATUS", "Failed", str(e), f"{raw}.log_run_details", )
	raise e
		
