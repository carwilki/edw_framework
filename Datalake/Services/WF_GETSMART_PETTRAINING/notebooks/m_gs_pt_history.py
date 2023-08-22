#Code converted on 2023-07-28 07:59:20
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
# Processing node SQ_Shortcut_to_GS_PT_HISTORY, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_GS_PT_HISTORY = spark.sql(f"""SELECT * FROM {legacy}.GS_PT_HISTORY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_PT_HISTORY_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_GS_PT_HISTORY_PRE = spark.sql(f"""SELECT * FROM {raw}.GS_PT_HISTORY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_PT_HISTORY_temp = SQ_Shortcut_to_GS_PT_HISTORY.toDF(*["SQ_Shortcut_to_GS_PT_HISTORY___" + col for col in SQ_Shortcut_to_GS_PT_HISTORY.columns])
SQ_Shortcut_to_GS_PT_HISTORY_PRE_temp = SQ_Shortcut_to_GS_PT_HISTORY_PRE.toDF(*["SQ_Shortcut_to_GS_PT_HISTORY_PRE___" + col for col in SQ_Shortcut_to_GS_PT_HISTORY_PRE.columns])

JNRTRANS = SQ_Shortcut_to_GS_PT_HISTORY_temp.join(SQ_Shortcut_to_GS_PT_HISTORY_PRE_temp,[SQ_Shortcut_to_GS_PT_HISTORY_temp.SQ_Shortcut_to_GS_PT_HISTORY___GS_PT_HISTORY_ID == SQ_Shortcut_to_GS_PT_HISTORY_PRE_temp.SQ_Shortcut_to_GS_PT_HISTORY_PRE___GS_PT_HISTORY_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___GS_PT_HISTORY_ID as GS_PT_HISTORY_ID",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___FIELD_NAME as FIELD_NAME",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___OLD_VALUE as OLD_VALUE",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___NEW_VALUE as NEW_VALUE",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___UPDATE_BY as UPDATE_BY",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___UPDATED_DT as UPDATED_DT",
	"SQ_Shortcut_to_GS_PT_HISTORY_PRE___ACTION as ACTION",
	"SQ_Shortcut_to_GS_PT_HISTORY___GS_PT_HISTORY_ID as GS_PT_HISTORY_ID1",
	"SQ_Shortcut_to_GS_PT_HISTORY___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID1",
	"SQ_Shortcut_to_GS_PT_HISTORY___FIELD_NAME as FIELD_NAME1",
	"SQ_Shortcut_to_GS_PT_HISTORY___OLD_VALUE as OLD_VALUE1",
	"SQ_Shortcut_to_GS_PT_HISTORY___NEW_VALUE as NEW_VALUE1",
	"SQ_Shortcut_to_GS_PT_HISTORY___UPDATE_BY as UPDATE_BY1",
	"SQ_Shortcut_to_GS_PT_HISTORY___UPDATED_DT as UPDATED_DT1",
	"SQ_Shortcut_to_GS_PT_HISTORY___ACTION as ACTION1",
	"SQ_Shortcut_to_GS_PT_HISTORY___LOAD_TSTMP as LOAD_TSTMP")

# COMMAND ----------
# Processing node EXP_MD5_UPD_FLAG, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXP_MD5_UPD_FLAG = JNRTRANS_temp\
	.withColumn("MD5_RESULTS", expr("""IF (MD5 ( concat ( JNRTRANS___GS_PT_TRAINING_ID , JNRTRANS___FIELD_NAME , JNRTRANS___OLD_VALUE , JNRTRANS___NEW_VALUE , JNRTRANS___UPDATE_BY , JNRTRANS___UPDATED_DT , JNRTRANS___ACTION ) ) != MD5 (concat( JNRTRANS___GS_PT_TRAINING_ID1 , JNRTRANS___FIELD_NAME1 , JNRTRANS___OLD_VALUE1 , JNRTRANS___NEW_VALUE1 , JNRTRANS___UPDATE_BY1 , JNRTRANS___UPDATED_DT1 , JNRTRANS___ACTION1 )), 1, 0)""")) \
	.withColumn("v_UPD_FLAG", expr("""IF (JNRTRANS___GS_PT_HISTORY_ID1 IS NULL, 0, IF (MD5_RESULTS = 1, 1, 3))"""))\
.selectExpr(
	"JNRTRANS___LOAD_TSTMP as i_LOAD_TSTMP",
	"JNRTRANS___GS_PT_HISTORY_ID as GS_PT_HISTORY_ID",
	"JNRTRANS___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"JNRTRANS___FIELD_NAME as FIELD_NAME",
	"JNRTRANS___OLD_VALUE as OLD_VALUE",
	"JNRTRANS___NEW_VALUE as NEW_VALUE",
	"JNRTRANS___UPDATE_BY as UPDATE_BY",
	"JNRTRANS___UPDATED_DT as UPDATED_DT",
	"JNRTRANS___ACTION as ACTION",
	"JNRTRANS___GS_PT_HISTORY_ID1 as GS_PT_HISTORY_ID1",
	"JNRTRANS___GS_PT_TRAINING_ID1 as GS_PT_TRAINING_ID1",
	"JNRTRANS___FIELD_NAME1 as FIELD_NAME1",
	"JNRTRANS___OLD_VALUE1 as OLD_VALUE1",
	"JNRTRANS___NEW_VALUE1 as NEW_VALUE1",
	"JNRTRANS___UPDATE_BY1 as UPDATE_BY1",
	"JNRTRANS___UPDATED_DT1 as UPDATED_DT1",
	"JNRTRANS___ACTION1 as ACTION1",
 	"v_UPD_FLAG as v_UPD_FLAG").selectExpr(
	"i_LOAD_TSTMP as i_LOAD_TSTMP",
	"v_UPD_FLAG as o_UPD_FLAG",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (v_UPD_FLAG = 1, i_LOAD_TSTMP, CURRENT_TIMESTAMP) as o_LOAD_TSTMP",
	"GS_PT_HISTORY_ID as GS_PT_HISTORY_ID",
	"GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"FIELD_NAME as FIELD_NAME",
	"OLD_VALUE as OLD_VALUE",
	"NEW_VALUE as NEW_VALUE",
	"UPDATE_BY as UPDATE_BY",
	"UPDATED_DT as UPDATED_DT",
	"ACTION as ACTION",
	"GS_PT_HISTORY_ID1 as GS_PT_HISTORY_ID1",
	"GS_PT_TRAINING_ID1 as GS_PT_TRAINING_ID1",
	"FIELD_NAME1 as FIELD_NAME1",
	"OLD_VALUE1 as OLD_VALUE1",
	"NEW_VALUE1 as NEW_VALUE1",
	"UPDATE_BY1 as UPDATE_BY1",
	"UPDATED_DT1 as UPDATED_DT1",
	"ACTION1 as ACTION1"
)

# COMMAND ----------
# Processing node FIL_UPD_FLAG, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_MD5_UPD_FLAG_temp = EXP_MD5_UPD_FLAG.toDF(*["EXP_MD5_UPD_FLAG___" + col for col in EXP_MD5_UPD_FLAG.columns])

FIL_UPD_FLAG = EXP_MD5_UPD_FLAG_temp.selectExpr(
	"EXP_MD5_UPD_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_MD5_UPD_FLAG___o_LOAD_TSTMP as o_LOAD_TSTMP",
	"EXP_MD5_UPD_FLAG___o_UPD_FLAG as o_UPD_FLAG",
	"EXP_MD5_UPD_FLAG___GS_PT_HISTORY_ID as GS_PT_HISTORY_ID",
	"EXP_MD5_UPD_FLAG___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"EXP_MD5_UPD_FLAG___FIELD_NAME as FIELD_NAME",
	"EXP_MD5_UPD_FLAG___OLD_VALUE as OLD_VALUE",
	"EXP_MD5_UPD_FLAG___NEW_VALUE as NEW_VALUE",
	"EXP_MD5_UPD_FLAG___UPDATE_BY as UPDATE_BY",
	"EXP_MD5_UPD_FLAG___UPDATED_DT as UPDATED_DT",
	"EXP_MD5_UPD_FLAG___ACTION as ACTION").filter("o_UPD_FLAG != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_UPD_FLAG, type UPDATE_STRATEGY 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_UPD_FLAG_temp = FIL_UPD_FLAG.toDF(*["FIL_UPD_FLAG___" + col for col in FIL_UPD_FLAG.columns])

UPD_UPD_FLAG = FIL_UPD_FLAG_temp.selectExpr(
	"FIL_UPD_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
	"FIL_UPD_FLAG___o_LOAD_TSTMP as o_LOAD_TSTMP",
	"FIL_UPD_FLAG___o_UPD_FLAG as o_UPD_FLAG",
	"FIL_UPD_FLAG___GS_PT_HISTORY_ID as GS_PT_HISTORY_ID",
	"FIL_UPD_FLAG___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"FIL_UPD_FLAG___FIELD_NAME as FIELD_NAME",
	"FIL_UPD_FLAG___OLD_VALUE as OLD_VALUE",
	"FIL_UPD_FLAG___NEW_VALUE as NEW_VALUE",
	"FIL_UPD_FLAG___UPDATE_BY as UPDATE_BY",
	"FIL_UPD_FLAG___UPDATED_DT as UPDATED_DT",
	"FIL_UPD_FLAG___ACTION as ACTION") \
	.withColumn('pyspark_data_action', col('o_UPD_FLAG'))

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_HISTORY1, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_GS_PT_HISTORY1 = UPD_UPD_FLAG.selectExpr(
	"GS_PT_HISTORY_ID as GS_PT_HISTORY_ID",
	"GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"CAST(FIELD_NAME AS STRING) as FIELD_NAME",
	"CAST(OLD_VALUE AS STRING) as OLD_VALUE",
	"CAST(NEW_VALUE AS STRING) as NEW_VALUE",
	"CAST(UPDATE_BY AS STRING) as UPDATE_BY",
	"CAST(UPDATED_DT AS TIMESTAMP) as UPDATED_DT",
	"CAST(ACTION AS STRING) as ACTION",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.GS_PT_HISTORY_ID = target.GS_PT_HISTORY_ID"""
	refined_perf_table = f"{legacy}.GS_PT_HISTORY"
	executeMerge(Shortcut_to_GS_PT_HISTORY1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GS_PT_HISTORY", "GS_PT_HISTORY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GS_PT_HISTORY", "GS_PT_HISTORY","Failed",str(e), f"{raw}.log_run_details")
	raise e
		