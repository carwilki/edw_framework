#Code converted on 2023-08-09 10:48:07
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
# Processing node SQ_Shortcut_to_TRAINING_RESERVATION, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_RESERVATION = spark.sql(f"""SELECT
TRAINING_RESERVATION_ID,
SRC_CREATE_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_RESERVATION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_RESERVATION_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_TRAINING_RESERVATION_PRE = spark.sql(f"""SELECT
RESERVATION_ID,
CUSTOMER_ID,
PET_ID,
STORE_CLASS_ID,
STORE_NUMBER,
ENROLLMENT_DATE_TIME,
CREATE_DATE_TIME,
PACKAGE_OPTION_ID
FROM {raw}.TRAINING_RESERVATION_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_RESERVATION, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_RESERVATION_PRE_temp = SQ_Shortcut_to_TRAINING_RESERVATION_PRE.toDF(*["SQ_Shortcut_to_TRAINING_RESERVATION_PRE___" + col for col in SQ_Shortcut_to_TRAINING_RESERVATION_PRE.columns])
SQ_Shortcut_to_TRAINING_RESERVATION_temp = SQ_Shortcut_to_TRAINING_RESERVATION.toDF(*["SQ_Shortcut_to_TRAINING_RESERVATION___" + col for col in SQ_Shortcut_to_TRAINING_RESERVATION.columns])

JNR_TRAINING_RESERVATION = SQ_Shortcut_to_TRAINING_RESERVATION_temp.join(SQ_Shortcut_to_TRAINING_RESERVATION_PRE_temp,[SQ_Shortcut_to_TRAINING_RESERVATION_temp.SQ_Shortcut_to_TRAINING_RESERVATION___TRAINING_RESERVATION_ID == SQ_Shortcut_to_TRAINING_RESERVATION_PRE_temp.SQ_Shortcut_to_TRAINING_RESERVATION_PRE___RESERVATION_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___RESERVATION_ID as RESERVATION_ID", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___CUSTOMER_ID as CUSTOMER_ID", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___PET_ID as PET_ID", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___STORE_CLASS_ID as STORE_CLASS_ID", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___STORE_NUMBER as STORE_NUMBER", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___ENROLLMENT_DATE_TIME as ENROLLMENT_DATE_TIME", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_TRAINING_RESERVATION_PRE___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"SQ_Shortcut_to_TRAINING_RESERVATION___TRAINING_RESERVATION_ID as lkp_TRAINING_RESERVATION_ID", \
	"SQ_Shortcut_to_TRAINING_RESERVATION___SRC_CREATE_TSTMP as lkp_SRC_CREATE_TSTMP", \
	"SQ_Shortcut_to_TRAINING_RESERVATION___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_RESERVATION_temp = JNR_TRAINING_RESERVATION.toDF(*["JNR_TRAINING_RESERVATION___" + col for col in JNR_TRAINING_RESERVATION.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_RESERVATION_temp.selectExpr( \
	"JNR_TRAINING_RESERVATION___RESERVATION_ID as RESERVATION_ID", \
	"JNR_TRAINING_RESERVATION___CUSTOMER_ID as CUSTOMER_ID", \
	"JNR_TRAINING_RESERVATION___PET_ID as PET_ID", \
	"JNR_TRAINING_RESERVATION___STORE_CLASS_ID as STORE_CLASS_ID", \
	"JNR_TRAINING_RESERVATION___STORE_NUMBER as STORE_NUMBER", \
	"JNR_TRAINING_RESERVATION___ENROLLMENT_DATE_TIME as ENROLLMENT_DATE_TIME", \
	"JNR_TRAINING_RESERVATION___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_TRAINING_RESERVATION___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"JNR_TRAINING_RESERVATION___lkp_TRAINING_RESERVATION_ID as lkp_TRAINING_RESERVATION_ID", \
	"JNR_TRAINING_RESERVATION___lkp_SRC_CREATE_TSTMP as lkp_SRC_CREATE_TSTMP", \
	"JNR_TRAINING_RESERVATION___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_TRAINING_RESERVATION_ID IS NULL OR ( lkp_TRAINING_RESERVATION_ID IS NOT NULL AND IF (CREATE_DATE_TIME IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), CREATE_DATE_TIME) != IF (lkp_SRC_CREATE_TSTMP IS NULL, To_DATE ( '12-31-9999' , 'MM-DD-YYYY' ), lkp_SRC_CREATE_TSTMP) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___RESERVATION_ID as RESERVATION_ID", \
	"FIL_UNCHANGED_RECORDS___CUSTOMER_ID as CUSTOMER_ID", \
	"FIL_UNCHANGED_RECORDS___PET_ID as PET_ID", \
	"FIL_UNCHANGED_RECORDS___STORE_CLASS_ID as STORE_CLASS_ID", \
	"FIL_UNCHANGED_RECORDS___STORE_NUMBER as STORE_NUMBER", \
	"FIL_UNCHANGED_RECORDS___ENROLLMENT_DATE_TIME as ENROLLMENT_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___lkp_TRAINING_RESERVATION_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___RESERVATION_ID as RESERVATION_ID", \
	"EXP_UPDATE_VALIDATOR___CUSTOMER_ID as CUSTOMER_ID", \
	"EXP_UPDATE_VALIDATOR___PET_ID as PET_ID", \
	"EXP_UPDATE_VALIDATOR___STORE_CLASS_ID as STORE_CLASS_ID", \
	"EXP_UPDATE_VALIDATOR___STORE_NUMBER as STORE_NUMBER", \
	"EXP_UPDATE_VALIDATOR___ENROLLMENT_DATE_TIME as ENROLLMENT_DATE_TIME", \
	"EXP_UPDATE_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPDATE_VALIDATOR___PACKAGE_OPTION_ID as PACKAGE_OPTION_ID", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPDATE_VALIDATOR___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_RESERVATION1, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_TRAINING_RESERVATION1 = UPD_INSERT_UPDATE.selectExpr( \
	"CAST(RESERVATION_ID AS INT) as TRAINING_RESERVATION_ID", \
	"CAST(CUSTOMER_ID AS INT) as TRAINING_CUSTOMER_ID", \
	"CAST(PET_ID AS INT) as TRAINING_PET_ID", \
	"CAST(STORE_CLASS_ID AS INT) as TRAINING_STORE_CLASS_ID", \
	"CAST(PACKAGE_OPTION_ID AS INT) as TRAINING_PACKAGE_OPTION_ID", \
	"CAST(ENROLLMENT_DATE_TIME AS TIMESTAMP) as TRAINING_ENROLLMENT_TSTMP", \
	"CAST(STORE_NUMBER AS BIGINT) as STORE_NBR", \
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as SRC_CREATE_TSTMP", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_RESERVATION1.write.saveAsTable(f'{raw}.TRAINING_RESERVATION', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_RESERVATION_ID = target.TRAINING_RESERVATION_ID"""
  refined_perf_table = f"{legacy}.TRAINING_RESERVATION"
  executeMerge(Shortcut_to_TRAINING_RESERVATION1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_RESERVATION", "TRAINING_RESERVATION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_RESERVATION", "TRAINING_RESERVATION","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	