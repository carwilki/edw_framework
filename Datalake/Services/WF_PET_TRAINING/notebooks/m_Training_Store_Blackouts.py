#Code converted on 2023-08-09 10:48:18
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
# Processing node SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE = spark.sql(f"""SELECT
STORE_NUMBER,
BLACK_OUT_START_DATE,
BLACK_OUT_END_DATE
FROM {raw}.TRAINING_STORE_BLACKOUTS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS = spark.sql(f"""SELECT
STORE_NBR,
BLACK_OUT_START_DT,
BLACK_OUT_END_DT,
DELETED_FLAG,
LOAD_TSTMP
FROM {legacy}.TRAINING_STORE_BLACKOUTS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_STORE_BLACKOUTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_temp = SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS.toDF(*["SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___" + col for col in SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS.columns])
SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE_temp = SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE.toDF(*["SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___" + col for col in SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE.columns])

JNR_TRAINING_STORE_BLACKOUTS = SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_temp.join(SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE_temp,[SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_temp.SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___STORE_NBR == SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE_temp.SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___STORE_NUMBER],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___STORE_NUMBER as STORE_NUMBER", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___BLACK_OUT_START_DATE as BLACK_OUT_START_DATE", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___BLACK_OUT_END_DATE as BLACK_OUT_END_DATE", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___STORE_NBR as lkp_STORE_NBR", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___BLACK_OUT_START_DT as lkp_BLACK_OUT_START_DT", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___BLACK_OUT_END_DT as lkp_BLACK_OUT_END_DT", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___LOAD_TSTMP as lkp_LOAD_TSTMP", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___DELETED_FLAG as lkp_DELETED_FLAG")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_STORE_BLACKOUTS_temp = JNR_TRAINING_STORE_BLACKOUTS.toDF(*["JNR_TRAINING_STORE_BLACKOUTS___" + col for col in JNR_TRAINING_STORE_BLACKOUTS.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_STORE_BLACKOUTS_temp.selectExpr( \
	"JNR_TRAINING_STORE_BLACKOUTS___STORE_NUMBER as STORE_NUMBER", \
	"JNR_TRAINING_STORE_BLACKOUTS___BLACK_OUT_START_DATE as BLACK_OUT_START_DATE", \
	"JNR_TRAINING_STORE_BLACKOUTS___BLACK_OUT_END_DATE as BLACK_OUT_END_DATE", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_STORE_NBR as lkp_STORE_NBR", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_BLACK_OUT_START_DT as lkp_BLACK_OUT_START_DT", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_BLACK_OUT_END_DT as lkp_BLACK_OUT_END_DT", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_DELETED_FLAG as lkp_DELETED_FLAG").filter("lkp_STORE_NBR IS NULL OR ( lkp_STORE_NBR IS NOT NULL AND ( IF (BLACK_OUT_START_DATE IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), BLACK_OUT_START_DATE) != IF (lkp_BLACK_OUT_START_DT IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_BLACK_OUT_START_DT) OR IF (BLACK_OUT_END_DATE IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), BLACK_OUT_END_DATE) != IF (lkp_BLACK_OUT_END_DT IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_BLACK_OUT_END_DT) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___STORE_NUMBER as STORE_NUMBER", \
	"FIL_UNCHANGED_RECORDS___BLACK_OUT_START_DATE as BLACK_OUT_START_DATE", \
	"FIL_UNCHANGED_RECORDS___BLACK_OUT_END_DATE as BLACK_OUT_END_DATE", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_STORE_NBR IS NULL, 1, 2) as UPDATE_FLAG", \
	"0 as DELETE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___STORE_NUMBER as STORE_NUMBER", \
	"EXP_UPDATE_VALIDATOR___BLACK_OUT_START_DATE as BLACK_OUT_START_DATE", \
	"EXP_UPDATE_VALIDATOR___BLACK_OUT_END_DATE as BLACK_OUT_END_DATE", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG", \
	"EXP_UPDATE_VALIDATOR___DELETE_FLAG as DELETE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_STORE_BLACKOUTS1, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_TRAINING_STORE_BLACKOUTS1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(STORE_NUMBER AS INT) as STORE_NBR", \
	"CAST(BLACK_OUT_START_DATE AS DATE) as BLACK_OUT_START_DT", \
	"CAST(BLACK_OUT_END_DATE AS DATE) as BLACK_OUT_END_DT", \
	"CAST(DELETE_FLAG AS TINYINT) as DELETED_FLAG", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_STORE_BLACKOUTS1.write.saveAsTable(f'{raw}.TRAINING_STORE_BLACKOUTS', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.STORE_NBR = target.STORE_NBR"""
  refined_perf_table = f"{legacy}.TRAINING_STORE_BLACKOUTS"
  executeMerge(Shortcut_to_TRAINING_STORE_BLACKOUTS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_STORE_BLACKOUTS", "TRAINING_STORE_BLACKOUTS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_STORE_BLACKOUTS", "TRAINING_STORE_BLACKOUTS","Failed",str(e), f"{raw}.log_run_details", )
  raise e