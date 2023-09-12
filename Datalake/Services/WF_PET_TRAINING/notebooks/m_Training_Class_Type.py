#Code converted on 2023-08-09 10:47:56
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
# Processing node SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE, type SOURCE 
# COLUMN COUNT: 14

SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE = spark.sql(f"""SELECT
CLASS_TYPE_ID,
NAME,
SHORT_DESCRIPTION,
DURATION,
PRICE,
UPC,
INFO_URL,
LAST_MODIFIED,
SORT_ORDER_ID,
IS_ACTIVE,
CATEGORY_ID,
DURATION_UNIT_OF_MEASURE_ID,
SESSION_LENGTH,
SESSION_UNIT_OF_MEASURE_ID
FROM {raw}.TRAINING_CLASS_TYPE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_CLASS_TYPE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_CLASS_TYPE = spark.sql(f"""SELECT
TRAINING_CLASS_TYPE_ID,
SRC_LAST_MODIFIED_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_CLASS_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_CLASS_TYPE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_CLASS_TYPE_temp = SQ_Shortcut_to_TRAINING_CLASS_TYPE.toDF(*["SQ_Shortcut_to_TRAINING_CLASS_TYPE___" + col for col in SQ_Shortcut_to_TRAINING_CLASS_TYPE.columns])
SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE_temp = SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE.toDF(*["SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___" + col for col in SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE.columns])

JNR_TRAINING_CLASS_TYPE = SQ_Shortcut_to_TRAINING_CLASS_TYPE_temp.join(SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE_temp,[SQ_Shortcut_to_TRAINING_CLASS_TYPE_temp.SQ_Shortcut_to_TRAINING_CLASS_TYPE___TRAINING_CLASS_TYPE_ID == SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE_temp.SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___CLASS_TYPE_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___CLASS_TYPE_ID as CLASS_TYPE_ID", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___NAME as NAME", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___SHORT_DESCRIPTION as SHORT_DESCRIPTION", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___DURATION as DURATION", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___PRICE as PRICE", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___UPC as UPC", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___INFO_URL as INFO_URL", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___LAST_MODIFIED as LAST_MODIFIED", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE___TRAINING_CLASS_TYPE_ID as lkp_TRAINING_CLASS_TYPE_ID", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE___SRC_LAST_MODIFIED_TSTMP as lkp_SRC_LAST_MODIFIED_TSTMP", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE___LOAD_TSTMP as lkp_LOAD_TSTMP", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___SORT_ORDER_ID as SORT_ORDER_ID", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___IS_ACTIVE as IS_ACTIVE", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___CATEGORY_ID as CATEGORY_ID", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___DURATION_UNIT_OF_MEASURE_ID as DURATION_UNIT_OF_MEASURE_ID", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___SESSION_LENGTH as SESSION_LENGTH", \
	"SQ_Shortcut_to_TRAINING_CLASS_TYPE_PRE___SESSION_UNIT_OF_MEASURE_ID as SESSION_UNIT_OF_MEASURE_ID")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_CLASS_TYPE_temp = JNR_TRAINING_CLASS_TYPE.toDF(*["JNR_TRAINING_CLASS_TYPE___" + col for col in JNR_TRAINING_CLASS_TYPE.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_CLASS_TYPE_temp.selectExpr( \
	"JNR_TRAINING_CLASS_TYPE___CLASS_TYPE_ID as CLASS_TYPE_ID", \
	"JNR_TRAINING_CLASS_TYPE___NAME as NAME", \
	"JNR_TRAINING_CLASS_TYPE___SHORT_DESCRIPTION as SHORT_DESCRIPTION", \
	"JNR_TRAINING_CLASS_TYPE___DURATION as DURATION", \
	"JNR_TRAINING_CLASS_TYPE___PRICE as PRICE", \
	"JNR_TRAINING_CLASS_TYPE___UPC as UPC", \
	"JNR_TRAINING_CLASS_TYPE___INFO_URL as INFO_URL", \
	"JNR_TRAINING_CLASS_TYPE___LAST_MODIFIED as LAST_MODIFIED", \
	"JNR_TRAINING_CLASS_TYPE___lkp_TRAINING_CLASS_TYPE_ID as lkp_TRAINING_CLASS_TYPE_ID", \
	"JNR_TRAINING_CLASS_TYPE___lkp_SRC_LAST_MODIFIED_TSTMP as lkp_SRC_LAST_MODIFIED_TSTMP", \
	"JNR_TRAINING_CLASS_TYPE___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP", \
	"JNR_TRAINING_CLASS_TYPE___SORT_ORDER_ID as SORT_ORDER_ID", \
	"JNR_TRAINING_CLASS_TYPE___IS_ACTIVE as IS_ACTIVE", \
	"JNR_TRAINING_CLASS_TYPE___CATEGORY_ID as CATEGORY_ID", \
	"JNR_TRAINING_CLASS_TYPE___DURATION_UNIT_OF_MEASURE_ID as DURATION_UNIT_OF_MEASURE_ID", \
	"JNR_TRAINING_CLASS_TYPE___SESSION_LENGTH as SESSION_LENGTH", \
	"JNR_TRAINING_CLASS_TYPE___SESSION_UNIT_OF_MEASURE_ID as SESSION_UNIT_OF_MEASURE_ID").filter("lkp_TRAINING_CLASS_TYPE_ID IS NULL OR ( lkp_TRAINING_CLASS_TYPE_ID IS NOT NULL AND ( IF (LAST_MODIFIED IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), LAST_MODIFIED) != IF (lkp_SRC_LAST_MODIFIED_TSTMP IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_SRC_LAST_MODIFIED_TSTMP) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___CLASS_TYPE_ID as CLASS_TYPE_ID", \
	"FIL_UNCHANGED_RECORDS___NAME as NAME", \
	"FIL_UNCHANGED_RECORDS___SHORT_DESCRIPTION as SHORT_DESCRIPTION", \
	"FIL_UNCHANGED_RECORDS___DURATION as DURATION", \
	"FIL_UNCHANGED_RECORDS___PRICE as PRICE", \
	"BIGINT(FIL_UNCHANGED_RECORDS___UPC) as o_UPC", \
	"FIL_UNCHANGED_RECORDS___INFO_URL as INFO_URL", \
	"FIL_UNCHANGED_RECORDS___LAST_MODIFIED as LAST_MODIFIED", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_CLASS_TYPE_ID IS NULL, 1, 2) as UPDATE_FLAG", \
	"FIL_UNCHANGED_RECORDS___SORT_ORDER_ID as SORT_ORDER_ID", \
	"FIL_UNCHANGED_RECORDS___IS_ACTIVE as IS_ACTIVE", \
	"FIL_UNCHANGED_RECORDS___CATEGORY_ID as CATEGORY_ID", \
	"FIL_UNCHANGED_RECORDS___DURATION_UNIT_OF_MEASURE_ID as DURATION_UNIT_OF_MEASURE_ID", \
	"FIL_UNCHANGED_RECORDS___SESSION_LENGTH as SESSION_LENGTH", \
	"FIL_UNCHANGED_RECORDS___SESSION_UNIT_OF_MEASURE_ID as SESSION_UNIT_OF_MEASURE_ID" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___CLASS_TYPE_ID as CLASS_TYPE_ID", \
	"EXP_UPDATE_VALIDATOR___NAME as NAME", \
	"EXP_UPDATE_VALIDATOR___SHORT_DESCRIPTION as SHORT_DESCRIPTION", \
	"EXP_UPDATE_VALIDATOR___DURATION as DURATION", \
	"EXP_UPDATE_VALIDATOR___PRICE as PRICE", \
	"EXP_UPDATE_VALIDATOR___o_UPC as UPC", \
	"EXP_UPDATE_VALIDATOR___INFO_URL as INFO_URL", \
	"EXP_UPDATE_VALIDATOR___LAST_MODIFIED as LAST_MODIFIED", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG", \
	"EXP_UPDATE_VALIDATOR___SORT_ORDER_ID as SORT_ORDER_ID", \
	"EXP_UPDATE_VALIDATOR___IS_ACTIVE as IS_ACTIVE", \
	"EXP_UPDATE_VALIDATOR___CATEGORY_ID as CATEGORY_ID", \
	"EXP_UPDATE_VALIDATOR___DURATION_UNIT_OF_MEASURE_ID as DURATION_UNIT_OF_MEASURE_ID", \
	"EXP_UPDATE_VALIDATOR___SESSION_LENGTH as SESSION_LENGTH", \
	"EXP_UPDATE_VALIDATOR___SESSION_UNIT_OF_MEASURE_ID as SESSION_UNIT_OF_MEASURE_ID") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CLASS_TYPE1, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_TRAINING_CLASS_TYPE1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(CLASS_TYPE_ID AS BIGINT) as TRAINING_CLASS_TYPE_ID", \
	"CAST(NAME AS STRING) as TRAINING_CLASS_TYPE_NAME", \
	"CAST(SHORT_DESCRIPTION AS STRING) as TRAINING_CLASS_TYPE_SHORT_DESC", \
	"CAST(UPC as INT) as UPC_ID", \
	"CAST(DURATION AS BIGINT) as TRAINING_CLASS_DURATION", \
	"CAST(INFO_URL AS STRING) as INFO_URL", \
	"CAST(PRICE AS DECIMAL(19,4)) as PRICE_AMT", \
	"CAST(LAST_MODIFIED AS TIMESTAMP) as SRC_LAST_MODIFIED_TSTMP", \
	"CAST(SORT_ORDER_ID AS BIGINT) as SORT_ORDER_ID", \
	"CAST(IS_ACTIVE AS BIGINT) as IS_ACTIVE", \
	"CAST(CATEGORY_ID AS BIGINT) as TRAINING_CATEGORY_ID", \
	"CAST(DURATION_UNIT_OF_MEASURE_ID AS BIGINT) as DURATION_UNIT_OF_MEASURE_ID", \
	"CAST(SESSION_LENGTH AS BIGINT) as SESSION_LENGTH", \
	"CAST(SESSION_UNIT_OF_MEASURE_ID AS BIGINT) as SESSION_UNIT_OF_MEASURE_ID", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_CLASS_TYPE1.write.saveAsTable(f'{raw}.TRAINING_CLASS_TYPE', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")
# Manually added  primary key as none specified
try:
  primary_key = """source.TRAINING_CLASS_TYPE_ID = target.TRAINING_CLASS_TYPE_ID"""
  refined_perf_table = f"{legacy}.TRAINING_CLASS_TYPE"
  executeMerge(Shortcut_to_TRAINING_CLASS_TYPE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_CLASS_TYPE", "TRAINING_CLASS_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_CLASS_TYPE", "TRAINING_CLASS_TYPE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	