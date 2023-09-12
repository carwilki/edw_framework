#Code converted on 2023-08-09 10:48:02
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
# Processing node SQ_Shortcut_to_TRAINING_PACKAGE_OPTION, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_PACKAGE_OPTION = spark.sql(f"""SELECT
TRAINING_PACKAGE_OPTION_ID,
SRC_LAST_MODIFIED_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_PACKAGE_OPTION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE = spark.sql(f"""SELECT
PACKAGE_OPTION_ID,
PACKAGE_ID,
NAME,
DISPLAY_NAME,
DESCRIPTION,
UPC,
PRICE,
LAST_MODIFIED
FROM {raw}.TRAINING_PACKAGE_OPTION_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_PACKAGE_OPTION, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_temp = SQ_Shortcut_to_TRAINING_PACKAGE_OPTION.toDF(*["SQ_Shortcut_to_TRAINING_PACKAGE_OPTION___" + col for col in SQ_Shortcut_to_TRAINING_PACKAGE_OPTION.columns])
SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE_temp = SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE.toDF(*["SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___" + col for col in SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE.columns])

JNR_TRAINING_PACKAGE_OPTION = SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_temp.join(SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE_temp,[SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_temp.SQ_Shortcut_to_TRAINING_PACKAGE_OPTION___TRAINING_PACKAGE_OPTION_ID == SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE_temp.SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___PACKAGE_OPTION_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___PACKAGE_ID as PACKAGE_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___NAME as NAME", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___DISPLAY_NAME as DISPLAY_NAME", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___UPC as UPC", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___PRICE as PRICE", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION_PRE___LAST_MODIFIED as LAST_MODIFIED", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION___TRAINING_PACKAGE_OPTION_ID as lkp_TRAINING_PACKAGE_OPTION_ID", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION___SRC_LAST_MODIFIED_TSTMP as lkp_SRC_LAST_MODIFIED_TSTMP", \
	"SQ_Shortcut_to_TRAINING_PACKAGE_OPTION___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_PACKAGE_OPTION_temp = JNR_TRAINING_PACKAGE_OPTION.toDF(*["JNR_TRAINING_PACKAGE_OPTION___" + col for col in JNR_TRAINING_PACKAGE_OPTION.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_PACKAGE_OPTION_temp.selectExpr( \
	"JNR_TRAINING_PACKAGE_OPTION___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"JNR_TRAINING_PACKAGE_OPTION___PACKAGE_ID as PACKAGE_ID", \
	"JNR_TRAINING_PACKAGE_OPTION___NAME as NAME", \
	"JNR_TRAINING_PACKAGE_OPTION___DISPLAY_NAME as DISPLAY_NAME", \
	"JNR_TRAINING_PACKAGE_OPTION___DESCRIPTION as DESCRIPTION", \
	"JNR_TRAINING_PACKAGE_OPTION___UPC as UPC", \
	"JNR_TRAINING_PACKAGE_OPTION___PRICE as PRICE", \
	"JNR_TRAINING_PACKAGE_OPTION___LAST_MODIFIED as LAST_MODIFIED", \
	"JNR_TRAINING_PACKAGE_OPTION___lkp_TRAINING_PACKAGE_OPTION_ID as lkp_TRAINING_PACKAGE_OPTION_ID", \
	"JNR_TRAINING_PACKAGE_OPTION___lkp_SRC_LAST_MODIFIED_TSTMP as lkp_SRC_LAST_MODIFIED_TSTMP", \
	"JNR_TRAINING_PACKAGE_OPTION___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_PACKAGE_OPTION_ID IS NULL OR ( lkp_TRAINING_PACKAGE_OPTION_ID IS NOT NULL AND IF (LAST_MODIFIED IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), LAST_MODIFIED) != IF (lkp_SRC_LAST_MODIFIED_TSTMP IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_SRC_LAST_MODIFIED_TSTMP) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"FIL_UNCHANGED_RECORDS___PACKAGE_ID as PACKAGE_ID", \
	"FIL_UNCHANGED_RECORDS___NAME as NAME", \
	"FIL_UNCHANGED_RECORDS___DISPLAY_NAME as DISPLAY_NAME", \
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", \
	"BIGINT(FIL_UNCHANGED_RECORDS___UPC) as o_UPC", \
	"FIL_UNCHANGED_RECORDS___PRICE as PRICE", \
	"FIL_UNCHANGED_RECORDS___LAST_MODIFIED as LAST_MODIFIED", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_PACKAGE_OPTION_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"EXP_UPDATE_VALIDATOR___PACKAGE_ID as PACKAGE_ID", \
	"EXP_UPDATE_VALIDATOR___NAME as NAME", \
	"EXP_UPDATE_VALIDATOR___DISPLAY_NAME as DISPLAY_NAME", \
	"EXP_UPDATE_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_UPDATE_VALIDATOR___o_UPC as UPC", \
	"EXP_UPDATE_VALIDATOR___PRICE as PRICE", \
	"EXP_UPDATE_VALIDATOR___LAST_MODIFIED as LAST_MODIFIED", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PACKAGE_OPTION1, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_TRAINING_PACKAGE_OPTION1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(PACKAGE_OPTION_ID AS BIGINT) as TRAINING_PACKAGE_OPTION_ID", \
	"CAST(NAME AS STRING) as TRAINING_PACKAGE_OPTION_NAME", \
	"CAST(DISPLAY_NAME AS STRING) as TRAINING_PACKAGE_OPTION_DISPLAY_NAME", \
	"CAST(DESCRIPTION AS STRING) as TRAINING_PACKAGE_OPTION_DESC", \
	"CAST(PACKAGE_ID AS BIGINT) as TRAINING_PACKAGE_ID", \
	"CAST(UPC as INT) as UPC_ID", \
	"CAST(PRICE AS DECIMAL(19,4)) as PRICE_AMT", \
	"CAST(LAST_MODIFIED AS TIMESTAMP) as SRC_LAST_MODIFIED_TSTMP", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_PACKAGE_OPTION1.write.saveAsTable(f'{raw}.TRAINING_PACKAGE_OPTION', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_PACKAGE_OPTION_ID = target.TRAINING_PACKAGE_OPTION_ID"""
  refined_perf_table = f"{legacy}.TRAINING_PACKAGE_OPTION"
  executeMerge(Shortcut_to_TRAINING_PACKAGE_OPTION1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_PACKAGE_OPTION", "TRAINING_PACKAGE_OPTION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_PACKAGE_OPTION", "TRAINING_PACKAGE_OPTION","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	