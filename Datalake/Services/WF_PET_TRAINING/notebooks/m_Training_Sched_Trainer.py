#Code converted on 2023-08-09 10:48:17
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
# Processing node SQ_Shortcut_to_TRAINING_SCHED_TRAINER, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_TRAINING_SCHED_TRAINER = spark.sql(f"""SELECT
TRAINING_SCHED_TRAINER_ID,
STORE_NBR,
EMPLOYEE_ID,
FIRST_NAME,
LAST_NAME,
ACTIVE_FLAG,
LAST_MODIFIED_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_SCHED_TRAINER""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE = spark.sql(f"""SELECT
TRAINER_ID,
STORE_NUMBER,
FIRST_NAME,
LAST_NAME,
ASSOCIATE_ID,
IS_ACTIVE,
LAST_MODIFIED
FROM {raw}.TRAINING_SCHED_TRAINER_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_SCHED_TRAINER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_SCHED_TRAINER_temp = SQ_Shortcut_to_TRAINING_SCHED_TRAINER.toDF(*["SQ_Shortcut_to_TRAINING_SCHED_TRAINER___" + col for col in SQ_Shortcut_to_TRAINING_SCHED_TRAINER.columns])
SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE_temp = SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE.toDF(*["SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___" + col for col in SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE.columns])

JNR_TRAINING_SCHED_TRAINER = SQ_Shortcut_to_TRAINING_SCHED_TRAINER_temp.join(SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE_temp,[SQ_Shortcut_to_TRAINING_SCHED_TRAINER_temp.SQ_Shortcut_to_TRAINING_SCHED_TRAINER___TRAINING_SCHED_TRAINER_ID == SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE_temp.SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___TRAINER_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___TRAINER_ID as TRAINER_ID", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___STORE_NUMBER as STORE_NUMBER", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___FIRST_NAME as FIRST_NAME", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___LAST_NAME as LAST_NAME", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___ASSOCIATE_ID as ASSOCIATE_ID", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___IS_ACTIVE as IS_ACTIVE", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER_PRE___LAST_MODIFIED as LAST_MODIFIED", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___TRAINING_SCHED_TRAINER_ID as lkp_TRAINING_SCHED_TRAINER_ID", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___STORE_NBR as lkp_STORE_NBR", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___EMPLOYEE_ID as lkp_EMPLOYEE_ID", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___FIRST_NAME as lkp_FIRST_NAME1", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___LAST_NAME as lkp_LAST_NAME1", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___ACTIVE_FLAG as lkp_ACTIVE_FLAG", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___LAST_MODIFIED_TSTMP as lkp_LAST_MODIFIED_TSTMP", \
	"SQ_Shortcut_to_TRAINING_SCHED_TRAINER___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_SCHED_TRAINER_temp = JNR_TRAINING_SCHED_TRAINER.toDF(*["JNR_TRAINING_SCHED_TRAINER___" + col for col in JNR_TRAINING_SCHED_TRAINER.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_SCHED_TRAINER_temp.selectExpr( \
	"JNR_TRAINING_SCHED_TRAINER___TRAINER_ID as TRAINER_ID", \
	"JNR_TRAINING_SCHED_TRAINER___STORE_NUMBER as STORE_NUMBER", \
	"JNR_TRAINING_SCHED_TRAINER___FIRST_NAME as FIRST_NAME", \
	"JNR_TRAINING_SCHED_TRAINER___LAST_NAME as LAST_NAME", \
	"JNR_TRAINING_SCHED_TRAINER___ASSOCIATE_ID as ASSOCIATE_ID", \
	"JNR_TRAINING_SCHED_TRAINER___IS_ACTIVE as IS_ACTIVE", \
	"JNR_TRAINING_SCHED_TRAINER___LAST_MODIFIED as LAST_MODIFIED", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_TRAINING_SCHED_TRAINER_ID as lkp_TRAINING_SCHED_TRAINER_ID", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_STORE_NBR as lkp_STORE_NBR", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_EMPLOYEE_ID as lkp_EMPLOYEE_ID", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_FIRST_NAME1 as lkp_FIRST_NAME1", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_LAST_NAME1 as lkp_LAST_NAME1", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_ACTIVE_FLAG as lkp_ACTIVE_FLAG", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_LAST_MODIFIED_TSTMP as lkp_LAST_MODIFIED_TSTMP", \
	"JNR_TRAINING_SCHED_TRAINER___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_SCHED_TRAINER_ID IS NULL OR ( lkp_TRAINING_SCHED_TRAINER_ID IS NOT NULL AND ( IF (STORE_NUMBER IS NULL, cast(99 as int), STORE_NUMBER) != IF (lkp_STORE_NBR IS NULL, cast(99 as int), lkp_STORE_NBR) OR IF (LTRIM ( RTRIM ( FIRST_NAME ) ) IS NULL, ' ', LTRIM ( RTRIM ( FIRST_NAME ) )) != IF (LTRIM ( RTRIM ( lkp_FIRST_NAME1 ) ) IS NULL, ' ', LTRIM ( RTRIM ( lkp_FIRST_NAME1 ) )) OR IF (LTRIM ( RTRIM ( LAST_NAME ) ) IS NULL, ' ', LTRIM ( RTRIM ( LAST_NAME ) )) != IF (LTRIM ( RTRIM ( lkp_LAST_NAME1 ) ) IS NULL, ' ', LTRIM ( RTRIM ( lkp_LAST_NAME1 ) )) OR IF (ASSOCIATE_ID IS NULL, cast(99 as int), ASSOCIATE_ID) != IF (lkp_EMPLOYEE_ID IS NULL, cast(99 as int), lkp_EMPLOYEE_ID) OR IF (IS_ACTIVE IS NULL, cast(99 as int), IS_ACTIVE) != IF (lkp_ACTIVE_FLAG IS NULL, cast(99 as int), lkp_ACTIVE_FLAG) OR IF (LAST_MODIFIED IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), LAST_MODIFIED) != IF (lkp_LAST_MODIFIED_TSTMP IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_LAST_MODIFIED_TSTMP) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___TRAINER_ID as TRAINER_ID", \
	"FIL_UNCHANGED_RECORDS___STORE_NUMBER as STORE_NUMBER", \
	"FIL_UNCHANGED_RECORDS___FIRST_NAME as FIRST_NAME", \
	"FIL_UNCHANGED_RECORDS___LAST_NAME as LAST_NAME", \
	"FIL_UNCHANGED_RECORDS___ASSOCIATE_ID as ASSOCIATE_ID", \
	"FIL_UNCHANGED_RECORDS___IS_ACTIVE as IS_ACTIVE", \
	"FIL_UNCHANGED_RECORDS___LAST_MODIFIED as LAST_MODIFIED", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_SCHED_TRAINER_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___TRAINER_ID as TRAINER_ID", \
	"EXP_UPDATE_VALIDATOR___STORE_NUMBER as STORE_NUMBER", \
	"EXP_UPDATE_VALIDATOR___FIRST_NAME as FIRST_NAME", \
	"EXP_UPDATE_VALIDATOR___LAST_NAME as LAST_NAME", \
	"EXP_UPDATE_VALIDATOR___ASSOCIATE_ID as ASSOCIATE_ID", \
	"EXP_UPDATE_VALIDATOR___IS_ACTIVE as IS_ACTIVE", \
	"EXP_UPDATE_VALIDATOR___LAST_MODIFIED as LAST_MODIFIED", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_SCHED_TRAINER1, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_TRAINING_SCHED_TRAINER1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(TRAINER_ID AS INT) as TRAINING_SCHED_TRAINER_ID", \
	"CAST(STORE_NUMBER AS INT) as STORE_NBR", \
	"CAST(ASSOCIATE_ID AS INT) as EMPLOYEE_ID", \
	"CAST(FIRST_NAME AS STRING) as FIRST_NAME", \
	"CAST(LAST_NAME AS STRING) as LAST_NAME", \
	"CAST(IS_ACTIVE AS TINYINT) as ACTIVE_FLAG", \
	"CAST(LAST_MODIFIED AS TIMESTAMP) as LAST_MODIFIED_TSTMP", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_SCHED_TRAINER1.write.saveAsTable(f'{raw}.TRAINING_SCHED_TRAINER', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_SCHED_TRAINER_ID = target.TRAINING_SCHED_TRAINER_ID"""
  refined_perf_table = f"{legacy}.TRAINING_SCHED_TRAINER"
  executeMerge(Shortcut_to_TRAINING_SCHED_TRAINER1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_SCHED_TRAINER", "TRAINING_SCHED_TRAINER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_SCHED_TRAINER", "TRAINING_SCHED_TRAINER","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	