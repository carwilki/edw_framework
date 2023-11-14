# Databricks notebook source
# Code converted on 2023-08-30 11:26:03
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

(username,password,connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_InlinePogGroups, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_InlinePogGroups = jdbcSqlServerConnection(f"""(SELECT *, 
row_number() over(partition by GroupId order by modifieddate asc ) as row_num 
FROM CKB_PRD.space_DDD.InlinePogGroups) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

SQ_Shortcut_to_InlinePogGroups_removeduplicates=SQ_Shortcut_to_InlinePogGroups.filter(col('row_num')==1)

# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_GROUP, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_POG_GROUP = spark.sql(f"""SELECT
POG_GROUP.POG_GROUP_ID,
POG_GROUP.POG_GROUP_DESC
FROM {legacy}.POG_GROUP""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_LeftJoin, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_InlinePogGroups_temp = SQ_Shortcut_to_InlinePogGroups_removeduplicates.toDF(*["SQ_Shortcut_to_InlinePogGroups___" + col for col in SQ_Shortcut_to_InlinePogGroups_removeduplicates.columns])
SQ_Shortcut_to_POG_GROUP_temp = SQ_Shortcut_to_POG_GROUP.toDF(*["SQ_Shortcut_to_POG_GROUP___" + col for col in SQ_Shortcut_to_POG_GROUP.columns])

JNR_LeftJoin = SQ_Shortcut_to_POG_GROUP_temp.join(SQ_Shortcut_to_InlinePogGroups_temp,[SQ_Shortcut_to_POG_GROUP_temp.SQ_Shortcut_to_POG_GROUP___POG_GROUP_ID == SQ_Shortcut_to_InlinePogGroups_temp.SQ_Shortcut_to_InlinePogGroups___groupid],'right_outer').selectExpr(
	"SQ_Shortcut_to_InlinePogGroups___GroupId as GroupId",
	"SQ_Shortcut_to_InlinePogGroups___Description as Description",
	"SQ_Shortcut_to_POG_GROUP___POG_GROUP_ID as POG_GROUP_ID",
	"SQ_Shortcut_to_POG_GROUP___POG_GROUP_DESC as POG_GROUP_DESC") \
	.withColumn('LOAD_DT', lit(None))
	

# COMMAND ----------

# Processing node EXP_CheckChanges, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_LeftJoin_temp = JNR_LeftJoin.toDF(*["JNR_LeftJoin___" + col for col in JNR_LeftJoin.columns])

EXP_CheckChanges = JNR_LeftJoin_temp.selectExpr(
	"JNR_LeftJoin___GroupId as GroupId",
	"JNR_LeftJoin___Description as Description",
	"JNR_LeftJoin___POG_GROUP_ID as POG_GROUP_ID",
	"JNR_LeftJoin___POG_GROUP_DESC as POG_GROUP_NM",
	"JNR_LeftJoin___LOAD_DT as LOAD_DT",
	"IF (JNR_LeftJoin___LOAD_DT IS NULL, CURRENT_TIMESTAMP, JNR_LeftJoin___LOAD_DT) as LOAD_DT_NOTNULL",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (JNR_LeftJoin___POG_GROUP_ID IS NULL, 0, IF (IF (JNR_LeftJoin___Description IS NULL, 'N\A', JNR_LeftJoin___Description) <> IF (JNR_LeftJoin___POG_GROUP_DESC IS NULL, 'N\A', JNR_LeftJoin___POG_GROUP_DESC), 1, 3)) as UPDATE_STRATEGY"
)

# COMMAND ----------

# Processing node FIL_RemoveRejected, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___GroupId as GroupId",
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
	"FIL_RemoveRejected___GroupId as GroupId",
	"FIL_RemoveRejected___Description as Description",
	"FIL_RemoveRejected___LOAD_DT_NOTNULL as LOAD_DT_NOTNULL",
	"FIL_RemoveRejected___UPDATE_DT as UPDATE_DT",
	"FIL_RemoveRejected___UPDATE_STRATEGY as UPDATE_STRATEGY",
	"FIL_RemoveRejected___UPDATE_STRATEGY as pyspark_data_action") 

# COMMAND ----------

# Processing node Shortcut_to_POG_GROUP_tgt, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_POG_GROUP_tgt = UPD_SetStrategy.selectExpr(
	"CAST(GroupId AS BIGINT) as POG_GROUP_ID",
	"CAST(Description AS STRING) as POG_GROUP_DESC",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT_NOTNULL AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.POG_GROUP_ID = target.POG_GROUP_ID"""
	refined_perf_table = f"{legacy}.POG_GROUP"
	executeMerge(Shortcut_to_POG_GROUP_tgt, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_GROUP", "POG_GROUP", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_GROUP", "POG_GROUP","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


