#Code converted on 2023-07-28 07:59:22
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
# Processing node SQ_Shortcut_to_GS_PT_TRAINING_TYPE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_GS_PT_TRAINING_TYPE = spark.sql(f"""SELECT * FROM {legacy}.GS_PT_TRAINING_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE = spark.sql(f"""SELECT * FROM {raw}.GS_PT_TRAINING_TYPE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_PT_TRAINING_TYPE_temp = SQ_Shortcut_to_GS_PT_TRAINING_TYPE.toDF(*["SQ_Shortcut_to_GS_PT_TRAINING_TYPE___" + col for col in SQ_Shortcut_to_GS_PT_TRAINING_TYPE.columns])
SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE_temp = SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE.toDF(*["SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE___" + col for col in SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE.columns])

JNRTRANS = SQ_Shortcut_to_GS_PT_TRAINING_TYPE_temp.join(SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE_temp,[SQ_Shortcut_to_GS_PT_TRAINING_TYPE_temp.SQ_Shortcut_to_GS_PT_TRAINING_TYPE___GS_PT_TRAINING_TYPE_ID == SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE_temp.SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE___GS_PT_TRAINING_TYPE_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE___GS_PT_TRAINING_TYPE_ID as GS_PT_TRAINING_TYPE_ID",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE___TRAINING_NAME as TRAINING_NAME",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE___CUT_OFF_DAYS as CUT_OFF_DAYS",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE_PRE___IS_ACTIVE as IS_ACTIVE",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE___GS_PT_TRAINING_TYPE_ID as GS_PT_TRAINING_TYPE_ID1",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE___TRAINING_NAME as TRAINING_NAME1",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE___CUT_OFF_DAYS as CUT_OFF_DAYS1",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE___IS_ACTIVE as IS_ACTIVE1",
	"SQ_Shortcut_to_GS_PT_TRAINING_TYPE___LOAD_TSTMP as LOAD_TSTMP")

# COMMAND ----------
# Processing node EXP_MD5_UPD_FLAG, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXP_MD5_UPD_FLAG = JNRTRANS_temp\
.withColumn("MD5_RESULTS", expr("""IF (MD5 ( concat ( JNRTRANS___TRAINING_NAME , JNRTRANS___CUT_OFF_DAYS , JNRTRANS___IS_ACTIVE ) ) != MD5 (concat( JNRTRANS___TRAINING_NAME1 , JNRTRANS___CUT_OFF_DAYS1 , JNRTRANS___IS_ACTIVE1 )), 1, 0)""")) \
	.withColumn("v_UPD_FLAG", expr("""IF (JNRTRANS___GS_PT_TRAINING_TYPE_ID1 IS NULL, 0, IF (MD5_RESULTS = 1, 1, 3))""")).selectExpr(
	"JNRTRANS___LOAD_TSTMP as i_LOAD_TSTMP",
	"JNRTRANS___GS_PT_TRAINING_TYPE_ID as GS_PT_TRAINING_TYPE_ID",
	"JNRTRANS___TRAINING_NAME as TRAINING_NAME",
	"JNRTRANS___CUT_OFF_DAYS as CUT_OFF_DAYS",
	"JNRTRANS___IS_ACTIVE as IS_ACTIVE",
	"JNRTRANS___GS_PT_TRAINING_TYPE_ID1 as GS_PT_TRAINING_TYPE_ID1",
	"JNRTRANS___TRAINING_NAME1 as TRAINING_NAME1",
	"JNRTRANS___CUT_OFF_DAYS1 as CUT_OFF_DAYS1",
	"JNRTRANS___IS_ACTIVE1 as IS_ACTIVE1",
 	"v_UPD_FLAG as v_UPD_FLAG").selectExpr(
	"i_LOAD_TSTMP as i_LOAD_TSTMP",
	"v_UPD_FLAG as o_UPD_FLAG",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (v_UPD_FLAG = 1, i_LOAD_TSTMP, CURRENT_TIMESTAMP) as o_LOAD_TSTMP",
	"GS_PT_TRAINING_TYPE_ID as GS_PT_TRAINING_TYPE_ID",
	"TRAINING_NAME as TRAINING_NAME",
	"CUT_OFF_DAYS as CUT_OFF_DAYS",
	"IS_ACTIVE as IS_ACTIVE",
	"GS_PT_TRAINING_TYPE_ID1 as GS_PT_TRAINING_TYPE_ID1",
	"TRAINING_NAME1 as TRAINING_NAME1",
	"CUT_OFF_DAYS1 as CUT_OFF_DAYS1",
	"IS_ACTIVE1 as IS_ACTIVE1"
)

# COMMAND ----------
# Processing node FIL_UPD_FLAG, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_MD5_UPD_FLAG_temp = EXP_MD5_UPD_FLAG.toDF(*["EXP_MD5_UPD_FLAG___" + col for col in EXP_MD5_UPD_FLAG.columns])

FIL_UPD_FLAG = EXP_MD5_UPD_FLAG_temp.selectExpr(
	"EXP_MD5_UPD_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_MD5_UPD_FLAG___o_LOAD_TSTMP as o_LOAD_TSTMP",
	"EXP_MD5_UPD_FLAG___o_UPD_FLAG as o_UPD_FLAG",
	"EXP_MD5_UPD_FLAG___GS_PT_TRAINING_TYPE_ID as GS_PT_TRAINING_TYPE_ID",
	"EXP_MD5_UPD_FLAG___TRAINING_NAME as TRAINING_NAME",
	"EXP_MD5_UPD_FLAG___CUT_OFF_DAYS as CUT_OFF_DAYS",
	"EXP_MD5_UPD_FLAG___IS_ACTIVE as IS_ACTIVE").filter("o_UPD_FLAG != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_UPD_FLAG, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_UPD_FLAG_temp = FIL_UPD_FLAG.toDF(*["FIL_UPD_FLAG___" + col for col in FIL_UPD_FLAG.columns])

UPD_UPD_FLAG = FIL_UPD_FLAG_temp.selectExpr(
	"FIL_UPD_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
	"FIL_UPD_FLAG___o_LOAD_TSTMP as o_LOAD_TSTMP",
	"FIL_UPD_FLAG___o_UPD_FLAG as o_UPD_FLAG",
	"FIL_UPD_FLAG___GS_PT_TRAINING_TYPE_ID as GS_PT_TRAINING_TYPE_ID",
	"FIL_UPD_FLAG___TRAINING_NAME as TRAINING_NAME",
	"FIL_UPD_FLAG___CUT_OFF_DAYS as CUT_OFF_DAYS",
	"FIL_UPD_FLAG___IS_ACTIVE as IS_ACTIVE") \
	.withColumn('pyspark_data_action', col('o_UPD_FLAG'))

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_TRAINING_TYPE, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_GS_PT_TRAINING_TYPE = UPD_UPD_FLAG.selectExpr(
	"CAST(GS_PT_TRAINING_TYPE_ID AS BIGINT) as GS_PT_TRAINING_TYPE_ID",
	"CAST(TRAINING_NAME AS STRING) as TRAINING_NAME",
	"CAST(CUT_OFF_DAYS AS BIGINT) as CUT_OFF_DAYS",
	"IS_ACTIVE as IS_ACTIVE",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.GS_PT_TRAINING_TYPE_ID = target.GS_PT_TRAINING_TYPE_ID"""
	refined_perf_table = f"{legacy}.GS_PT_TRAINING_TYPE"
	executeMerge(Shortcut_to_GS_PT_TRAINING_TYPE, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GS_PT_TRAINING_TYPE", "GS_PT_TRAINING_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GS_PT_TRAINING_TYPE", "GS_PT_TRAINING_TYPE","Failed",str(e), f"{raw}.log_run_details")
	raise e
		