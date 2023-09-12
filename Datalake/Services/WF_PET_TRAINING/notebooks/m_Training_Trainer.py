#Code converted on 2023-08-09 10:48:22
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
# Processing node SQ_Shortcut_to_TRAINING_TRAINER_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_TRAINER_PRE = spark.sql(f"""SELECT
TRAINER_ID,
FORMATTED_NAME,
IS_DELETED
FROM {raw}.TRAINING_TRAINER_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_TRAINER, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_TRAINING_TRAINER = spark.sql(f"""SELECT
TRAINING_TRAINER_ID,
TRAINING_TRAINER_NAME,
DELETED_FLAG,
LOAD_TSTMP
FROM {legacy}.TRAINING_TRAINER""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_TRAINER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_TRAINER_temp = SQ_Shortcut_to_TRAINING_TRAINER.toDF(*["SQ_Shortcut_to_TRAINING_TRAINER___" + col for col in SQ_Shortcut_to_TRAINING_TRAINER.columns])
SQ_Shortcut_to_TRAINING_TRAINER_PRE_temp = SQ_Shortcut_to_TRAINING_TRAINER_PRE.toDF(*["SQ_Shortcut_to_TRAINING_TRAINER_PRE___" + col for col in SQ_Shortcut_to_TRAINING_TRAINER_PRE.columns])

JNR_TRAINING_TRAINER = SQ_Shortcut_to_TRAINING_TRAINER_temp.join(SQ_Shortcut_to_TRAINING_TRAINER_PRE_temp,[SQ_Shortcut_to_TRAINING_TRAINER_temp.SQ_Shortcut_to_TRAINING_TRAINER___TRAINING_TRAINER_ID == SQ_Shortcut_to_TRAINING_TRAINER_PRE_temp.SQ_Shortcut_to_TRAINING_TRAINER_PRE___TRAINER_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_TRAINER_PRE___TRAINER_ID as TRAINER_ID", \
	"SQ_Shortcut_to_TRAINING_TRAINER_PRE___FORMATTED_NAME as FORMATTED_NAME", \
	"SQ_Shortcut_to_TRAINING_TRAINER_PRE___IS_DELETED as IS_DELETED", \
	"SQ_Shortcut_to_TRAINING_TRAINER___TRAINING_TRAINER_ID as lkp_TRAINING_TRAINER_ID", \
	"SQ_Shortcut_to_TRAINING_TRAINER___TRAINING_TRAINER_NAME as lkp_TRAINING_TRAINER_NAME", \
	"SQ_Shortcut_to_TRAINING_TRAINER___DELETED_FLAG as lkp_DELETED_FLAG", \
	"SQ_Shortcut_to_TRAINING_TRAINER___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_TRAINER_temp = JNR_TRAINING_TRAINER.toDF(*["JNR_TRAINING_TRAINER___" + col for col in JNR_TRAINING_TRAINER.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_TRAINER_temp.selectExpr( \
	"JNR_TRAINING_TRAINER___TRAINER_ID as TRAINER_ID", \
	"JNR_TRAINING_TRAINER___FORMATTED_NAME as FORMATTED_NAME", \
	"JNR_TRAINING_TRAINER___IS_DELETED as IS_DELETED", \
	"JNR_TRAINING_TRAINER___lkp_TRAINING_TRAINER_ID as lkp_TRAINING_TRAINER_ID", \
	"JNR_TRAINING_TRAINER___lkp_TRAINING_TRAINER_NAME as lkp_TRAINING_TRAINER_NAME", \
	"JNR_TRAINING_TRAINER___lkp_DELETED_FLAG as lkp_DELETED_FLAG", \
	"JNR_TRAINING_TRAINER___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_TRAINER_ID IS NULL OR ( lkp_TRAINING_TRAINER_ID IS NOT NULL AND ( IF (LTRIM ( RTRIM ( FORMATTED_NAME ) ) IS NULL, ' ', LTRIM ( RTRIM ( FORMATTED_NAME ) )) != IF (LTRIM ( RTRIM ( lkp_TRAINING_TRAINER_NAME ) ) IS NULL, ' ', LTRIM ( RTRIM ( lkp_TRAINING_TRAINER_NAME ) )) OR IF (IS_DELETED IS NULL, cast(99 as int), IS_DELETED) != IF (lkp_DELETED_FLAG IS NULL, cast(99 as int), lkp_DELETED_FLAG) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___TRAINER_ID as TRAINER_ID", \
	"FIL_UNCHANGED_RECORDS___FORMATTED_NAME as FORMATTED_NAME", \
	"FIL_UNCHANGED_RECORDS___IS_DELETED as IS_DELETED", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_TRAINER_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___TRAINER_ID as TRAINER_ID", \
	"EXP_UPDATE_VALIDATOR___FORMATTED_NAME as FORMATTED_NAME", \
	"EXP_UPDATE_VALIDATOR___IS_DELETED as IS_DELETED", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_TRAINER1, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_TRAINING_TRAINER1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(TRAINER_ID AS INT) as TRAINING_TRAINER_ID", \
	"CAST(FORMATTED_NAME AS STRING) as TRAINING_TRAINER_NAME", \
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_TRAINER1.write.saveAsTable(f'{raw}.TRAINING_TRAINER', mode = 'overwrite')

# spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")


try:
  primary_key = """source.TRAINING_TRAINER_ID = target.TRAINING_TRAINER_ID"""
  refined_perf_table = f"{legacy}.TRAINING_TRAINER"
  executeMerge(Shortcut_to_TRAINING_TRAINER1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_TRAINER", "TRAINING_TRAINER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_TRAINER", "TRAINING_TRAINER","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	