#Code converted on 2023-08-07 16:26:04
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_ACTIVITY_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TS_ACTIVITY_PRE = spark.sql(f"""SELECT
TS_ACTIVITY_PRE.ACTIVITYID,
TS_ACTIVITY_PRE.ACTIVITYNAME,
TS_ACTIVITY_PRE.ACTIVITYDESC
FROM {raw}.TS_ACTIVITY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_ACTIVITY, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_TS_ACTIVITY = spark.sql(f"""SELECT
TS_ACTIVITY.TS_ACTIVITY_ID,
TS_ACTIVITY.TS_ACTIVITY_NAME,
TS_ACTIVITY.TS_ACTIVITY_DESC,
TS_ACTIVITY.LOAD_TSTMP
FROM {legacy}.TS_ACTIVITY""").withColumn("sys_row_id2", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_CheckChanges, type JOINER 
# COLUMN COUNT: 7

JNR_CheckChanges = SQ_Shortcut_to_TS_ACTIVITY.join(SQ_Shortcut_to_TS_ACTIVITY_PRE,[SQ_Shortcut_to_TS_ACTIVITY.TS_ACTIVITY_ID == SQ_Shortcut_to_TS_ACTIVITY_PRE.ACTIVITYID],'right_outer')

# COMMAND ----------
# Processing node EXP_CheckChanges, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
JNR_CheckChanges_temp = JNR_CheckChanges.toDF(*["JNR_CheckChanges___" + col for col in JNR_CheckChanges.columns])

EXP_CheckChanges = JNR_CheckChanges_temp.selectExpr(
	"JNR_CheckChanges___sys_row_id as sys_row_id",
	"JNR_CheckChanges___ACTIVITYID as ACTIVITYID",
	"JNR_CheckChanges___ACTIVITYNAME as ACTIVITYNAME",
	"JNR_CheckChanges___ACTIVITYDESC as ACTIVITYDESC",
	"JNR_CheckChanges___TS_ACTIVITY_ID as TS_ACTIVITY_ID",
	"JNR_CheckChanges___TS_ACTIVITY_NAME as TS_ACTIVITY_NAME",
	"JNR_CheckChanges___TS_ACTIVITY_DESC as TS_ACTIVITY_DESC",
	"IF (JNR_CheckChanges___TS_ACTIVITY_ID IS NULL, 0, IF (IF (JNR_CheckChanges___ACTIVITYNAME IS NULL, 'XNULL', JNR_CheckChanges___ACTIVITYNAME) <> IF (JNR_CheckChanges___TS_ACTIVITY_NAME IS NULL, 'XNULL', JNR_CheckChanges___TS_ACTIVITY_NAME) OR IF (JNR_CheckChanges___ACTIVITYDESC IS NULL, 'XNULL', JNR_CheckChanges___ACTIVITYDESC) <> IF (JNR_CheckChanges___TS_ACTIVITY_DESC IS NULL, 'XNULL', JNR_CheckChanges___TS_ACTIVITY_DESC), 1, 3)) as UpdateStrategy",
	"CURRENT_TIMESTAMP as UpdateDt",
	"JNR_CheckChanges___LOAD_TSTMP as LOAD_TSTMP",
	"IF (JNR_CheckChanges___LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_CheckChanges___LOAD_TSTMP) as LOAD_TSTMP_NN"
)

# COMMAND ----------
# Processing node FIL_RemoveRejected, type FILTER 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___ACTIVITYID as ACTIVITYID",
	"EXP_CheckChanges___ACTIVITYNAME as ACTIVITYNAME",
	"EXP_CheckChanges___ACTIVITYDESC as ACTIVITYDESC",
	"EXP_CheckChanges___UpdateStrategy as UpdateStrategy",
	"EXP_CheckChanges___UpdateDt as UpdateDt",
	"EXP_CheckChanges___LOAD_TSTMP_NN as LOAD_TSTMP_NN").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_SetStrategy, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
FIL_RemoveRejected_temp = FIL_RemoveRejected.toDF(*["FIL_RemoveRejected___" + col for col in FIL_RemoveRejected.columns])

UPD_SetStrategy = FIL_RemoveRejected_temp.selectExpr(
	"FIL_RemoveRejected___ACTIVITYID as ACTIVITYID",
	"FIL_RemoveRejected___ACTIVITYNAME as ACTIVITYNAME",
	"FIL_RemoveRejected___ACTIVITYDESC as ACTIVITYDESC",
	"FIL_RemoveRejected___UpdateStrategy as UpdateStrategy",
	"FIL_RemoveRejected___UpdateDt as UpdateDt",
	"FIL_RemoveRejected___LOAD_TSTMP_NN as LOAD_TSTMP",
	"FIL_RemoveRejected___UpdateStrategy as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_tgt, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_TS_ACTIVITY_tgt = UPD_SetStrategy.selectExpr(
	"CAST(ACTIVITYID AS BIGINT) as TS_ACTIVITY_ID",
	"CAST(ACTIVITYNAME AS STRING) as TS_ACTIVITY_NAME",
	"CAST(ACTIVITYDESC AS STRING) as TS_ACTIVITY_DESC",
	"CAST(UpdateDt AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.TS_ACTIVITY_ID = target.TS_ACTIVITY_ID"""
	refined_perf_table = f"{legacy}.TS_ACTIVITY"
	executeMerge(Shortcut_to_TS_ACTIVITY_tgt, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("TS_ACTIVITY", "TS_ACTIVITY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TS_ACTIVITY", "TS_ACTIVITY","Failed",str(e), f"{raw}.log_run_details")
	raise e
		