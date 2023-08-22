#Code converted on 2023-07-28 07:59:23
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
# Processing node SQ_Shortcut_to_GS_PT_TRAINING_PRE, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_GS_PT_TRAINING_PRE = spark.sql(f"""SELECT * FROM {raw}.GS_PT_TRAINING_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_GS_PT_TRAINING, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_GS_PT_TRAINING = spark.sql(f"""SELECT * FROM {legacy}.GS_PT_TRAINING""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GS_PT_TRAINING_temp = SQ_Shortcut_to_GS_PT_TRAINING.toDF(*["SQ_Shortcut_to_GS_PT_TRAINING___" + col for col in SQ_Shortcut_to_GS_PT_TRAINING.columns])
SQ_Shortcut_to_GS_PT_TRAINING_PRE_temp = SQ_Shortcut_to_GS_PT_TRAINING_PRE.toDF(*["SQ_Shortcut_to_GS_PT_TRAINING_PRE___" + col for col in SQ_Shortcut_to_GS_PT_TRAINING_PRE.columns])

JNRTRANS = SQ_Shortcut_to_GS_PT_TRAINING_temp.join(SQ_Shortcut_to_GS_PT_TRAINING_PRE_temp,[SQ_Shortcut_to_GS_PT_TRAINING_temp.SQ_Shortcut_to_GS_PT_TRAINING___GS_PT_TRAINING_ID == SQ_Shortcut_to_GS_PT_TRAINING_PRE_temp.SQ_Shortcut_to_GS_PT_TRAINING_PRE___GS_PT_TRAINING_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___HOME_STORE_ID as HOME_STORE_ID",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___ASSOCIATE_ID as ASSOCIATE_ID",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___ASSOCIATE_FIRST_NAME as ASSOCIATE_FIRST_NAME",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___ASSOCIATE_LAST_NAME as ASSOCIATE_LAST_NAME",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___DM as DM",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___TRAINING_STORE_ID as TRAINING_STORE_ID",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___TRAINING_START_DT as TRAINING_START_DT",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___TRAINING_END_DT as TRAINING_END_DT",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___TRAINING_STATUS as TRAINING_STATUS",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___TRAINING_TYPE as TRAINING_TYPE",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___CREATED_DT as CREATED_DT",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___CREATED_BY as CREATED_BY",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___MODIFIED_BY as MODIFIED_BY",
	"SQ_Shortcut_to_GS_PT_TRAINING_PRE___MODIFIED_DT as MODIFIED_DT",
	"SQ_Shortcut_to_GS_PT_TRAINING___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID1",
	"SQ_Shortcut_to_GS_PT_TRAINING___GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID1",
	"SQ_Shortcut_to_GS_PT_TRAINING___HOME_STORE_ID as HOME_STORE_ID1",
	"SQ_Shortcut_to_GS_PT_TRAINING___ASSOCIATE_ID as ASSOCIATE_ID1",
	"SQ_Shortcut_to_GS_PT_TRAINING___ASSOCIATE_FIRST_NAME as ASSOCIATE_FIRST_NAME1",
	"SQ_Shortcut_to_GS_PT_TRAINING___ASSOCIATE_LAST_NAME as ASSOCIATE_LAST_NAME1",
	"SQ_Shortcut_to_GS_PT_TRAINING___DM as DM1",
	"SQ_Shortcut_to_GS_PT_TRAINING___TRAINING_STORE_ID as TRAINING_STORE_ID1",
	"SQ_Shortcut_to_GS_PT_TRAINING___TRAINING_START_DT as TRAINING_START_DT1",
	"SQ_Shortcut_to_GS_PT_TRAINING___TRAINING_END_DT as TRAINING_END_DT1",
	"SQ_Shortcut_to_GS_PT_TRAINING___TRAINING_STATUS as TRAINING_STATUS1",
	"SQ_Shortcut_to_GS_PT_TRAINING___TRAINING_TYPE as TRAINING_TYPE1",
	"SQ_Shortcut_to_GS_PT_TRAINING___CREATED_DT as CREATED_DT1",
	"SQ_Shortcut_to_GS_PT_TRAINING___CREATED_BY as CREATED_BY1",
	"SQ_Shortcut_to_GS_PT_TRAINING___MODIFIED_BY as MODIFIED_BY1",
	"SQ_Shortcut_to_GS_PT_TRAINING___MODIFIED_DT as MODIFIED_DT1",
	"SQ_Shortcut_to_GS_PT_TRAINING___LOAD_TSTMP as LOAD_TSTMP")

# COMMAND ----------
# Processing node EXP_MD5_UPD_FLAG, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXP_MD5_UPD_FLAG = JNRTRANS_temp\
  .withColumn("MD5_RESULTS", expr("""IF (MD5 ( concat ( JNRTRANS___GS_PT_TRAVEL_ID , JNRTRANS___HOME_STORE_ID , JNRTRANS___ASSOCIATE_ID , JNRTRANS___ASSOCIATE_FIRST_NAME , JNRTRANS___ASSOCIATE_LAST_NAME , JNRTRANS___DM , JNRTRANS___TRAINING_STORE_ID , JNRTRANS___TRAINING_START_DT , JNRTRANS___TRAINING_END_DT , JNRTRANS___TRAINING_STATUS , JNRTRANS___TRAINING_TYPE , JNRTRANS___CREATED_DT , JNRTRANS___CREATED_BY , JNRTRANS___MODIFIED_BY , JNRTRANS___MODIFIED_DT ) ) != MD5 (concat( JNRTRANS___GS_PT_TRAVEL_ID1 , JNRTRANS___HOME_STORE_ID1 , JNRTRANS___ASSOCIATE_ID1 , JNRTRANS___ASSOCIATE_FIRST_NAME1 , JNRTRANS___ASSOCIATE_LAST_NAME1 , JNRTRANS___DM1 , JNRTRANS___TRAINING_STORE_ID1 , JNRTRANS___TRAINING_START_DT1 , JNRTRANS___TRAINING_END_DT1 , JNRTRANS___TRAINING_STATUS1 , JNRTRANS___TRAINING_TYPE1 , JNRTRANS___CREATED_DT1 , JNRTRANS___CREATED_BY1 , JNRTRANS___MODIFIED_BY1 , JNRTRANS___MODIFIED_DT1 )), 1, 0)""")) \
	.withColumn("v_UPD_FLAG", expr("""IF (JNRTRANS___GS_PT_TRAINING_ID1 IS NULL, 0, IF (MD5_RESULTS = 1, 1, 3))""")).selectExpr(
	"JNRTRANS___LOAD_TSTMP as i_LOAD_TSTMP",
	"JNRTRANS___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"JNRTRANS___GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID",
	"JNRTRANS___HOME_STORE_ID as HOME_STORE_ID",
	"JNRTRANS___ASSOCIATE_ID as ASSOCIATE_ID",
	"JNRTRANS___ASSOCIATE_FIRST_NAME as ASSOCIATE_FIRST_NAME",
	"JNRTRANS___ASSOCIATE_LAST_NAME as ASSOCIATE_LAST_NAME",
	"JNRTRANS___DM as DM",
	"JNRTRANS___TRAINING_STORE_ID as TRAINING_STORE_ID",
	"JNRTRANS___TRAINING_START_DT as TRAINING_START_DT",
	"JNRTRANS___TRAINING_END_DT as TRAINING_END_DT",
	"JNRTRANS___TRAINING_STATUS as TRAINING_STATUS",
	"JNRTRANS___TRAINING_TYPE as TRAINING_TYPE",
	"JNRTRANS___CREATED_DT as CREATED_DT",
	"JNRTRANS___CREATED_BY as CREATED_BY",
	"JNRTRANS___MODIFIED_BY as MODIFIED_BY",
	"JNRTRANS___MODIFIED_DT as MODIFIED_DT",
	"JNRTRANS___GS_PT_TRAINING_ID1 as GS_PT_TRAINING_ID1",
	"JNRTRANS___GS_PT_TRAVEL_ID1 as GS_PT_TRAVEL_ID1",
	"JNRTRANS___HOME_STORE_ID1 as HOME_STORE_ID1",
	"JNRTRANS___ASSOCIATE_ID1 as ASSOCIATE_ID1",
	"JNRTRANS___ASSOCIATE_FIRST_NAME1 as ASSOCIATE_FIRST_NAME1",
	"JNRTRANS___ASSOCIATE_LAST_NAME1 as ASSOCIATE_LAST_NAME1",
	"JNRTRANS___DM1 as DM1",
	"JNRTRANS___TRAINING_STORE_ID1 as TRAINING_STORE_ID1",
	"JNRTRANS___TRAINING_START_DT1 as TRAINING_START_DT1",
	"JNRTRANS___TRAINING_END_DT1 as TRAINING_END_DT1",
	"JNRTRANS___TRAINING_STATUS1 as TRAINING_STATUS1",
	"JNRTRANS___TRAINING_TYPE1 as TRAINING_TYPE1",
	"JNRTRANS___CREATED_DT1 as CREATED_DT1",
	"JNRTRANS___CREATED_BY1 as CREATED_BY1",
	"JNRTRANS___MODIFIED_BY1 as MODIFIED_BY1",
	"JNRTRANS___MODIFIED_DT1 as MODIFIED_DT1",
 	"v_UPD_FLAG as v_UPD_FLAG").selectExpr(
	"i_LOAD_TSTMP as i_LOAD_TSTMP",
	"v_UPD_FLAG as o_UPD_FLAG",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (v_UPD_FLAG = 1, i_LOAD_TSTMP, CURRENT_TIMESTAMP) as o_LOAD_TSTMP",
	"GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID",
	"HOME_STORE_ID as HOME_STORE_ID",
	"ASSOCIATE_ID as ASSOCIATE_ID",
	"ASSOCIATE_FIRST_NAME as ASSOCIATE_FIRST_NAME",
	"ASSOCIATE_LAST_NAME as ASSOCIATE_LAST_NAME",
	"DM as DM",
	"TRAINING_STORE_ID as TRAINING_STORE_ID",
	"TRAINING_START_DT as TRAINING_START_DT",
	"TRAINING_END_DT as TRAINING_END_DT",
	"TRAINING_STATUS as TRAINING_STATUS",
	"TRAINING_TYPE as TRAINING_TYPE",
	"CREATED_DT as CREATED_DT",
	"CREATED_BY as CREATED_BY",
	"MODIFIED_BY as MODIFIED_BY",
	"MODIFIED_DT as MODIFIED_DT",
	"GS_PT_TRAINING_ID1 as GS_PT_TRAINING_ID1",
	"GS_PT_TRAVEL_ID1 as GS_PT_TRAVEL_ID1",
	"HOME_STORE_ID1 as HOME_STORE_ID1",
	"ASSOCIATE_ID1 as ASSOCIATE_ID1",
	"ASSOCIATE_FIRST_NAME1 as ASSOCIATE_FIRST_NAME1",
	"ASSOCIATE_LAST_NAME1 as ASSOCIATE_LAST_NAME1",
	"DM1 as DM1",
	"TRAINING_STORE_ID1 as TRAINING_STORE_ID1",
	"TRAINING_START_DT1 as TRAINING_START_DT1",
	"TRAINING_END_DT1 as TRAINING_END_DT1",
	"TRAINING_STATUS1 as TRAINING_STATUS1",
	"TRAINING_TYPE1 as TRAINING_TYPE1",
	"CREATED_DT1 as CREATED_DT1",
	"CREATED_BY1 as CREATED_BY1",
	"MODIFIED_BY1 as MODIFIED_BY1",
	"MODIFIED_DT1 as MODIFIED_DT1"
)

# COMMAND ----------
# Processing node FIL_UPD_FLAG, type FILTER 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
EXP_MD5_UPD_FLAG_temp = EXP_MD5_UPD_FLAG.toDF(*["EXP_MD5_UPD_FLAG___" + col for col in EXP_MD5_UPD_FLAG.columns])

FIL_UPD_FLAG = EXP_MD5_UPD_FLAG_temp.selectExpr(
	"EXP_MD5_UPD_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_MD5_UPD_FLAG___o_LOAD_TSTMP as o_LOAD_TSTMP",
	"EXP_MD5_UPD_FLAG___o_UPD_FLAG as o_UPD_FLAG",
	"EXP_MD5_UPD_FLAG___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"EXP_MD5_UPD_FLAG___GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID",
	"EXP_MD5_UPD_FLAG___HOME_STORE_ID as HOME_STORE_ID",
	"EXP_MD5_UPD_FLAG___ASSOCIATE_ID as ASSOCIATE_ID",
	"EXP_MD5_UPD_FLAG___ASSOCIATE_FIRST_NAME as ASSOCIATE_FIRST_NAME",
	"EXP_MD5_UPD_FLAG___ASSOCIATE_LAST_NAME as ASSOCIATE_LAST_NAME",
	"EXP_MD5_UPD_FLAG___DM as DM",
	"EXP_MD5_UPD_FLAG___TRAINING_STORE_ID as TRAINING_STORE_ID",
	"EXP_MD5_UPD_FLAG___TRAINING_START_DT as TRAINING_START_DT",
	"EXP_MD5_UPD_FLAG___TRAINING_END_DT as TRAINING_END_DT",
	"EXP_MD5_UPD_FLAG___TRAINING_STATUS as TRAINING_STATUS",
	"EXP_MD5_UPD_FLAG___TRAINING_TYPE as TRAINING_TYPE",
	"EXP_MD5_UPD_FLAG___CREATED_DT as CREATED_DT",
	"EXP_MD5_UPD_FLAG___CREATED_BY as CREATED_BY",
	"EXP_MD5_UPD_FLAG___MODIFIED_BY as MODIFIED_BY",
	"EXP_MD5_UPD_FLAG___MODIFIED_DT as MODIFIED_DT").filter("o_UPD_FLAG != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_UPD_FLAG, type UPDATE_STRATEGY 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
FIL_UPD_FLAG_temp = FIL_UPD_FLAG.toDF(*["FIL_UPD_FLAG___" + col for col in FIL_UPD_FLAG.columns])

UPD_UPD_FLAG = FIL_UPD_FLAG_temp.selectExpr(
	"FIL_UPD_FLAG___UPDATE_TSTMP as UPDATE_TSTMP",
	"FIL_UPD_FLAG___o_LOAD_TSTMP as o_LOAD_TSTMP",
	"FIL_UPD_FLAG___o_UPD_FLAG as o_UPD_FLAG",
	"FIL_UPD_FLAG___GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"FIL_UPD_FLAG___GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID",
	"FIL_UPD_FLAG___HOME_STORE_ID as HOME_STORE_ID",
	"FIL_UPD_FLAG___ASSOCIATE_ID as ASSOCIATE_ID",
	"FIL_UPD_FLAG___ASSOCIATE_FIRST_NAME as ASSOCIATE_FIRST_NAME",
	"FIL_UPD_FLAG___ASSOCIATE_LAST_NAME as ASSOCIATE_LAST_NAME",
	"FIL_UPD_FLAG___DM as DM",
	"FIL_UPD_FLAG___TRAINING_STORE_ID as TRAINING_STORE_ID",
	"FIL_UPD_FLAG___TRAINING_START_DT as TRAINING_START_DT",
	"FIL_UPD_FLAG___TRAINING_END_DT as TRAINING_END_DT",
	"FIL_UPD_FLAG___TRAINING_STATUS as TRAINING_STATUS",
	"FIL_UPD_FLAG___TRAINING_TYPE as TRAINING_TYPE",
	"FIL_UPD_FLAG___CREATED_DT as CREATED_DT",
	"FIL_UPD_FLAG___CREATED_BY as CREATED_BY",
	"FIL_UPD_FLAG___MODIFIED_BY as MODIFIED_BY",
	"FIL_UPD_FLAG___MODIFIED_DT as MODIFIED_DT") \
	.withColumn('pyspark_data_action', col('o_UPD_FLAG'))

# COMMAND ----------
# Processing node Shortcut_to_GS_PT_TRAINING1, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_GS_PT_TRAINING1 = UPD_UPD_FLAG.selectExpr(
	"GS_PT_TRAINING_ID as GS_PT_TRAINING_ID",
	"GS_PT_TRAVEL_ID as GS_PT_TRAVEL_ID",
	"HOME_STORE_ID as HOME_STORE_ID",
	"CAST(ASSOCIATE_ID AS STRING) as ASSOCIATE_ID",
	"CAST(ASSOCIATE_FIRST_NAME AS STRING) as ASSOCIATE_FIRST_NAME",
	"CAST(ASSOCIATE_LAST_NAME AS STRING) as ASSOCIATE_LAST_NAME",
	"CAST(DM AS STRING) as DM",
	"TRAINING_STORE_ID as TRAINING_STORE_ID",
	"CAST(TRAINING_START_DT AS TIMESTAMP) as TRAINING_START_DT",
	"CAST(TRAINING_END_DT AS TIMESTAMP) as TRAINING_END_DT",
	"CAST(TRAINING_STATUS AS BIGINT) as TRAINING_STATUS",
	"CAST(TRAINING_TYPE AS BIGINT) as TRAINING_TYPE",
	"CAST(CREATED_DT AS TIMESTAMP) as CREATED_DT",
	"CAST(CREATED_BY AS STRING) as CREATED_BY",
	"CAST(MODIFIED_BY AS STRING) as MODIFIED_BY",
	"CAST(MODIFIED_DT AS TIMESTAMP) as MODIFIED_DT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.GS_PT_TRAINING_ID = target.GS_PT_TRAINING_ID"""
	refined_perf_table = f"{legacy}.GS_PT_TRAINING"
	executeMerge(Shortcut_to_GS_PT_TRAINING1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GS_PT_TRAINING", "GS_PT_TRAINING", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GS_PT_TRAINING", "GS_PT_TRAINING","Failed",str(e), f"{raw}.log_run_details")
	raise e
		