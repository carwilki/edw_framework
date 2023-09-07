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
# Processing node SQ_Shortcut_to_TS_ACTIVITY_CATEGORY_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_TS_ACTIVITY_CATEGORY_PRE = spark.sql(f"""SELECT
TS_ACTIVITY_CATEGORY_PRE.ACTCATEGORYID,
TS_ACTIVITY_CATEGORY_PRE.ACTCATDESC
FROM {raw}.TS_ACTIVITY_CATEGORY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_ACTIVITY_CATEGORY, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TS_ACTIVITY_CATEGORY = spark.sql(f"""SELECT
TS_ACTIVITY_CATEGORY.TS_ACTIVITY_CAT_ID,
TS_ACTIVITY_CATEGORY.TS_ACTIVITY_CAT_DESC,
TS_ACTIVITY_CATEGORY.LOAD_TSTMP
FROM {legacy}.TS_ACTIVITY_CATEGORY""").withColumn("sys_row_id2", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_CheckChanges, type JOINER 
# COLUMN COUNT: 5

JNR_CheckChanges = SQ_Shortcut_to_TS_ACTIVITY_CATEGORY.join(SQ_Shortcut_to_TS_ACTIVITY_CATEGORY_PRE,[SQ_Shortcut_to_TS_ACTIVITY_CATEGORY.TS_ACTIVITY_CAT_ID == SQ_Shortcut_to_TS_ACTIVITY_CATEGORY_PRE.ACTCATEGORYID],'right_outer')

# COMMAND ----------
# Processing node EXP_CheckChanges, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_CheckChanges_temp = JNR_CheckChanges.toDF(*["JNR_CheckChanges___" + col for col in JNR_CheckChanges.columns])

EXP_CheckChanges = JNR_CheckChanges_temp.selectExpr(
	"JNR_CheckChanges___sys_row_id as sys_row_id",
	"JNR_CheckChanges___ACTCATEGORYID as ACTCATEGORYID",
	"JNR_CheckChanges___ACTCATDESC as ACTCATDESC",
	"JNR_CheckChanges___TS_ACTIVITY_CAT_ID as TS_ACTIVITY_CAT_ID",
	"JNR_CheckChanges___TS_ACTIVITY_CAT_DESC as TS_ACTIVITY_CAT_DESC",
	"IF (JNR_CheckChanges___TS_ACTIVITY_CAT_ID IS NULL, 0, IF (JNR_CheckChanges___ACTCATDESC <> JNR_CheckChanges___TS_ACTIVITY_CAT_DESC, 1, 3)) as UpdateStrategy",
	"CURRENT_TIMESTAMP as UpdateDt",
	"JNR_CheckChanges___LOAD_TSTMP as LOAD_TSTMP",
	"IF (JNR_CheckChanges___LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_CheckChanges___LOAD_TSTMP) as LOAD_TSTMP_NN"
)

# COMMAND ----------
# Processing node FIL_RemoveRejected, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___ACTCATEGORYID as ACTCATEGORYID",
	"EXP_CheckChanges___ACTCATDESC as ACTCATDESC",
	"EXP_CheckChanges___UpdateStrategy as UpdateStrategy",
	"EXP_CheckChanges___UpdateDt as UpdateDt",
	"EXP_CheckChanges___LOAD_TSTMP_NN as LOAD_TSTMP_NN").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_SetStrategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FIL_RemoveRejected_temp = FIL_RemoveRejected.toDF(*["FIL_RemoveRejected___" + col for col in FIL_RemoveRejected.columns])

UPD_SetStrategy = FIL_RemoveRejected_temp.selectExpr(
	"FIL_RemoveRejected___ACTCATEGORYID as ACTCATEGORYID",
	"FIL_RemoveRejected___ACTCATDESC as ACTCATDESC",
	"FIL_RemoveRejected___UpdateStrategy as UpdateStrategy",
	"FIL_RemoveRejected___UpdateDt as UpdateDt",
	"FIL_RemoveRejected___LOAD_TSTMP_NN as LOAD_TSTMP_NN",
 	"FIL_RemoveRejected___UpdateStrategy as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_CATEGORY_tgt, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_TS_ACTIVITY_CATEGORY_tgt = UPD_SetStrategy.selectExpr(
	"CAST(ACTCATEGORYID AS BIGINT) as TS_ACTIVITY_CAT_ID",
	"CAST(ACTCATDESC AS STRING) as TS_ACTIVITY_CAT_DESC",
	"CAST(UpdateDt AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_NN AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.TS_ACTIVITY_CAT_ID = target.TS_ACTIVITY_CAT_ID"""
	refined_perf_table = f"{legacy}.TS_ACTIVITY_CATEGORY"
	executeMerge(Shortcut_to_TS_ACTIVITY_CATEGORY_tgt, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("TS_ACTIVITY_CATEGORY", "TS_ACTIVITY_CATEGORY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TS_ACTIVITY_CATEGORY", "TS_ACTIVITY_CATEGORY","Failed",str(e), f"{raw}.log_run_details")
	raise e
		