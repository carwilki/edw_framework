#Code converted on 2023-08-09 10:48:05
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
sensitive = getEnvPrefix(env) + 'cust_sensitive'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_PET_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_TRAINING_PET_PRE = spark.sql(f"""SELECT
PET_ID,
PROVIDER_ID,
NAME,
BREED,
BIRTH_DATE,
NOTES,
CREATE_DATE_TIME,
EXTERNAL_PET_ID
FROM {sensitive}.raw_TRAINING_PET_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_PET, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_PET = spark.sql(f"""SELECT
TRAINING_PET_ID,
SRC_CREATE_TSTMP,
LOAD_TSTMP
FROM {sensitive}.legacy_TRAINING_PET""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_PET, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_PET_temp = SQ_Shortcut_to_TRAINING_PET.toDF(*["SQ_Shortcut_to_TRAINING_PET___" + col for col in SQ_Shortcut_to_TRAINING_PET.columns])
SQ_Shortcut_to_TRAINING_PET_PRE_temp = SQ_Shortcut_to_TRAINING_PET_PRE.toDF(*["SQ_Shortcut_to_TRAINING_PET_PRE___" + col for col in SQ_Shortcut_to_TRAINING_PET_PRE.columns])

JNR_TRAINING_PET = SQ_Shortcut_to_TRAINING_PET_temp.join(SQ_Shortcut_to_TRAINING_PET_PRE_temp,[SQ_Shortcut_to_TRAINING_PET_temp.SQ_Shortcut_to_TRAINING_PET___TRAINING_PET_ID == SQ_Shortcut_to_TRAINING_PET_PRE_temp.SQ_Shortcut_to_TRAINING_PET_PRE___PET_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_PET_PRE___PET_ID as PET_ID", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___PROVIDER_ID as PROVIDER_ID", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___NAME as NAME", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___BREED as BREED", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___BIRTH_DATE as BIRTH_DATE", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___NOTES as NOTES", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_TRAINING_PET_PRE___EXTERNAL_PET_ID as EXTERNAL_PET_ID", \
	"SQ_Shortcut_to_TRAINING_PET___TRAINING_PET_ID as lkp_TRAINING_PET_ID", \
	"SQ_Shortcut_to_TRAINING_PET___SRC_CREATE_TSTMP as lkp_SRC_CREATE_TSTMP", \
	"SQ_Shortcut_to_TRAINING_PET___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_PET_temp = JNR_TRAINING_PET.toDF(*["JNR_TRAINING_PET___" + col for col in JNR_TRAINING_PET.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_PET_temp.selectExpr( \
	"JNR_TRAINING_PET___PET_ID as PET_ID", \
	"JNR_TRAINING_PET___PROVIDER_ID as PROVIDER_ID", \
	"JNR_TRAINING_PET___NAME as NAME", \
	"JNR_TRAINING_PET___BREED as BREED", \
	"JNR_TRAINING_PET___BIRTH_DATE as BIRTH_DATE", \
	"JNR_TRAINING_PET___NOTES as NOTES", \
	"JNR_TRAINING_PET___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_TRAINING_PET___EXTERNAL_PET_ID as EXTERNAL_PET_ID", \
	"JNR_TRAINING_PET___lkp_TRAINING_PET_ID as lkp_TRAINING_PET_ID", \
	"JNR_TRAINING_PET___lkp_SRC_CREATE_TSTMP as lkp_SRC_CREATE_TSTMP", \
	"JNR_TRAINING_PET___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_PET_ID IS NULL OR ( lkp_TRAINING_PET_ID IS NOT NULL AND IF (CREATE_DATE_TIME IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), CREATE_DATE_TIME) != IF (lkp_SRC_CREATE_TSTMP IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_SRC_CREATE_TSTMP) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___PET_ID as PET_ID", \
	"FIL_UNCHANGED_RECORDS___PROVIDER_ID as PROVIDER_ID", \
	"FIL_UNCHANGED_RECORDS___NAME as NAME", \
	"FIL_UNCHANGED_RECORDS___BREED as BREED", \
	"FIL_UNCHANGED_RECORDS___BIRTH_DATE as BIRTH_DATE", \
	"FIL_UNCHANGED_RECORDS___NOTES as NOTES", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___EXTERNAL_PET_ID as EXTERNAL_PET_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_PET_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___PET_ID as PET_ID", \
	"EXP_UPDATE_VALIDATOR___PROVIDER_ID as PROVIDER_ID", \
	"EXP_UPDATE_VALIDATOR___NAME as NAME", \
	"EXP_UPDATE_VALIDATOR___BREED as BREED", \
	"EXP_UPDATE_VALIDATOR___BIRTH_DATE as BIRTH_DATE", \
	"EXP_UPDATE_VALIDATOR___NOTES as NOTES", \
	"EXP_UPDATE_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPDATE_VALIDATOR___EXTERNAL_PET_ID as EXTERNAL_PET_ID", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_PET1, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_TRAINING_PET1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(PET_ID AS BIGINT) as TRAINING_PET_ID", \
	"CAST(EXTERNAL_PET_ID as INT) as TRAINING_EXT_PET_ID", \
	"CAST(PROVIDER_ID AS STRING) as TRAINING_PROVIDER_ID", \
	"CAST(NAME AS STRING) as TRAINING_PET_NAME", \
	"CAST(BREED AS STRING) as PET_BREED", \
	"CAST(BIRTH_DATE AS DATE) as PET_BIRTH_DT", \
	"CAST(NOTES AS STRING) as NOTES", \
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as SRC_CREATE_TSTMP", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_PET1.write.saveAsTable(f'{raw}.TRAINING_PET', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_PET_ID = target.TRAINING_PET_ID"""
  refined_perf_table = f"{sensitive}.legacy_TRAINING_PET"
  executeMerge(Shortcut_to_TRAINING_PET1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_PET", "TRAINING_PET", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_PET", "TRAINING_PET","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	
