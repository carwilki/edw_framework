#Code converted on 2023-08-09 10:48:11
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
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE = spark.sql(f"""SELECT
CHANGE_STATE_ID,
CHANGE_STATE_NAME
FROM {raw}.TRAINING_SCHED_CHANGE_STATE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE = spark.sql(f"""SELECT
TRAINING_SCHED_CHANGE_STATE_ID,
TRAINING_SCHED_CHANGE_STATE_NAME,
LOAD_TSTMP
FROM {legacy}.TRAINING_SCHED_CHANGE_STATE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_SCHED_CHANGE_STATE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_temp = SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE.toDF(*["SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE___" + col for col in SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE.columns])
SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE_temp = SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE.toDF(*["SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE___" + col for col in SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE.columns])

JNR_TRAINING_SCHED_CHANGE_STATE = SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_temp.join(SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE_temp,[SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_temp.SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE___TRAINING_SCHED_CHANGE_STATE_ID == SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE_temp.SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE___CHANGE_STATE_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE___CHANGE_STATE_ID as CHANGE_STATE_ID", \
	"SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE_PRE___CHANGE_STATE_NAME as CHANGE_STATE_NAME", \
	"SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE___TRAINING_SCHED_CHANGE_STATE_ID as lkp_TRAINING_SCHED_CHANGE_STATE_ID", \
	"SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE___TRAINING_SCHED_CHANGE_STATE_NAME as lkp_TRAINING_SCHED_CHANGE_STATE_NAME", \
	"SQ_Shortcut_to_TRAINING_SCHED_CHANGE_STATE___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_SCHED_CHANGE_STATE_temp = JNR_TRAINING_SCHED_CHANGE_STATE.toDF(*["JNR_TRAINING_SCHED_CHANGE_STATE___" + col for col in JNR_TRAINING_SCHED_CHANGE_STATE.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_SCHED_CHANGE_STATE_temp.selectExpr( \
	"JNR_TRAINING_SCHED_CHANGE_STATE___CHANGE_STATE_ID as CHANGE_STATE_ID", \
	"JNR_TRAINING_SCHED_CHANGE_STATE___CHANGE_STATE_NAME as CHANGE_STATE_NAME", \
	"JNR_TRAINING_SCHED_CHANGE_STATE___lkp_TRAINING_SCHED_CHANGE_STATE_ID as lkp_TRAINING_SCHED_CHANGE_STATE_ID", \
	"JNR_TRAINING_SCHED_CHANGE_STATE___lkp_TRAINING_SCHED_CHANGE_STATE_NAME as lkp_TRAINING_SCHED_CHANGE_STATE_NAME", \
	"JNR_TRAINING_SCHED_CHANGE_STATE___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_SCHED_CHANGE_STATE_ID IS NULL OR ( lkp_TRAINING_SCHED_CHANGE_STATE_ID IS NOT NULL AND ( IF (LTRIM ( RTRIM ( CHANGE_STATE_NAME ) ) IS NULL, ' ', LTRIM ( RTRIM ( CHANGE_STATE_NAME ) )) != IF (LTRIM ( RTRIM ( lkp_TRAINING_SCHED_CHANGE_STATE_NAME ) ) IS NULL, ' ', LTRIM ( RTRIM ( lkp_TRAINING_SCHED_CHANGE_STATE_NAME ) )) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___CHANGE_STATE_ID as CHANGE_STATE_ID", \
	"FIL_UNCHANGED_RECORDS___CHANGE_STATE_NAME as CHANGE_STATE_NAME", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_SCHED_CHANGE_STATE_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___CHANGE_STATE_ID as CHANGE_STATE_ID", \
	"EXP_UPDATE_VALIDATOR___CHANGE_STATE_NAME as CHANGE_STATE_NAME", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_CHANGE_STATE1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_TRAINING_SCHED_CHANGE_STATE1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(CHANGE_STATE_ID AS INT) as TRAINING_SCHED_CHANGE_STATE_ID", \
	"CAST(CHANGE_STATE_NAME AS STRING) as TRAINING_SCHED_CHANGE_STATE_NAME", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_SCHED_CHANGE_STATE1.write.saveAsTable(f'{raw}.TRAINING_SCHED_CHANGE_STATE', mode = 'overwrite')
# spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_SCHED_CHANGE_STATE_ID = target.TRAINING_SCHED_CHANGE_STATE_ID"""
  refined_perf_table = f"{legacy}.TRAINING_SCHED_CHANGE_STATE"
  executeMerge(Shortcut_to_TRAINING_SCHED_CHANGE_STATE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_SCHED_CHANGE_STATE", "TRAINING_SCHED_CHANGE_STATE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_SCHED_CHANGE_STATE", "TRAINING_SCHED_CHANGE_STATE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	