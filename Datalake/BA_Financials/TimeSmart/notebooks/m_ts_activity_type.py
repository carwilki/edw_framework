#Code converted on 2023-08-07 16:26:05
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
# Processing node SQ_Shortcut_to_TS_ACTIVITY_TYPE_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_TS_ACTIVITY_TYPE_PRE = spark.sql(f"""SELECT
TS_ACTIVITY_TYPE_PRE.ACTTYPEID,
TS_ACTIVITY_TYPE_PRE.ACTIVITYTYPE
FROM {raw}.TS_ACTIVITY_TYPE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_ACTIVITY_TYPE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TS_ACTIVITY_TYPE = spark.sql(f"""SELECT
TS_ACTIVITY_TYPE.TS_ACTIVITY_TYPE_ID,
TS_ACTIVITY_TYPE.TS_ACTIVITY_TYPE_DESC,
TS_ACTIVITY_TYPE.LOAD_TSTMP
FROM {legacy}.TS_ACTIVITY_TYPE""").withColumn("sys_row_id2", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_CheckDates, type JOINER 
# COLUMN COUNT: 5

JNR_CheckDates = SQ_Shortcut_to_TS_ACTIVITY_TYPE.join(SQ_Shortcut_to_TS_ACTIVITY_TYPE_PRE,[SQ_Shortcut_to_TS_ACTIVITY_TYPE.TS_ACTIVITY_TYPE_ID == SQ_Shortcut_to_TS_ACTIVITY_TYPE_PRE.ACTTYPEID],'right_outer')

# COMMAND ----------
# Processing node EXP_CheckChanges, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_CheckDates_temp = JNR_CheckDates.toDF(*["JNR_CheckDates___" + col for col in JNR_CheckDates.columns])

EXP_CheckChanges = JNR_CheckDates_temp.selectExpr(
	"JNR_CheckDates___sys_row_id as sys_row_id",
	"JNR_CheckDates___ACTTYPEID as ACTTYPEID",
	"JNR_CheckDates___ACTIVITYTYPE as ACTIVITYTYPE",
	"JNR_CheckDates___TS_ACTIVITY_TYPE_ID as TS_ACTIVITY_TYPE_ID",
	"JNR_CheckDates___TS_ACTIVITY_TYPE_DESC as TS_ACTIVITY_TYPE_DESC",
	"IF (JNR_CheckDates___TS_ACTIVITY_TYPE_ID IS NULL, 0, IF (JNR_CheckDates___ACTIVITYTYPE <> JNR_CheckDates___TS_ACTIVITY_TYPE_DESC, 1, 3)) as UpdateStrategy",
	"CURRENT_TIMESTAMP as UpdateDt",
	"JNR_CheckDates___LOAD_TSTMP as LOAD_TSTMP",
	"IF (JNR_CheckDates___LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_CheckDates___LOAD_TSTMP) as LOAD_TSTMP_NN"
)

# COMMAND ----------
# Processing node FIL_RemoveRejected, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___ACTTYPEID as ACTTYPEID",
	"EXP_CheckChanges___ACTIVITYTYPE as ACTIVITYTYPE",
	"EXP_CheckChanges___UpdateStrategy as UpdateStrategy",
	"EXP_CheckChanges___UpdateDt as UpdateDt",
	"EXP_CheckChanges___LOAD_TSTMP_NN as LOAD_TSTMP_NN").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_SetStrategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FIL_RemoveRejected_temp = FIL_RemoveRejected.toDF(*["FIL_RemoveRejected___" + col for col in FIL_RemoveRejected.columns])

UPD_SetStrategy = FIL_RemoveRejected_temp.selectExpr(
	"FIL_RemoveRejected___ACTTYPEID as ACTTYPEID",
	"FIL_RemoveRejected___ACTIVITYTYPE as ACTIVITYTYPE",
	"FIL_RemoveRejected___UpdateStrategy as UpdateStrategy",
	"FIL_RemoveRejected___UpdateDt as UpdateDt",
	"FIL_RemoveRejected___LOAD_TSTMP_NN as LOAD_TSTMP_NN",
	"FIL_RemoveRejected___UpdateStrategy as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_TYPE_tgt, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_TS_ACTIVITY_TYPE_tgt = UPD_SetStrategy.selectExpr(
	"CAST(ACTTYPEID AS BIGINT) as TS_ACTIVITY_TYPE_ID",
	"CAST(ACTIVITYTYPE AS STRING) as TS_ACTIVITY_TYPE_DESC",
	"CAST(UpdateDt AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_NN AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.TS_ACTIVITY_TYPE_ID = target.TS_ACTIVITY_TYPE_ID"""
	refined_perf_table = f"{legacy}.TS_ACTIVITY_TYPE"
	executeMerge(Shortcut_to_TS_ACTIVITY_TYPE_tgt, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("TS_ACTIVITY_TYPE", "TS_ACTIVITY_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TS_ACTIVITY_TYPE", "TS_ACTIVITY_TYPE","Failed",str(e), f"{raw}.log_run_details")
	raise e
		