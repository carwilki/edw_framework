#Code converted on 2023-06-26 10:17:50
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script

refined_perf_table = f"{refine}.WM_LABOR_TRAN_DTL_CRIT"
raw_perf_table = f"{raw}.WM_LABOR_TRAN_DTL_CRIT_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT = spark.sql(f"""SELECT
WM_LABOR_TRAN_DTL_CRIT.LOCATION_ID,
WM_LABOR_TRAN_DTL_CRIT.WM_LABOR_TRAN_DTL_CRIT_ID,
WM_LABOR_TRAN_DTL_CRIT.WM_CREATED_TSTMP,
WM_LABOR_TRAN_DTL_CRIT.WM_LAST_UPDATED_TSTMP,
WM_LABOR_TRAN_DTL_CRIT.LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_LABOR_TRAN_DTL_CRIT_ID IN (SELECT LABOR_TRAN_DTL_CRIT_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE, type SOURCE 
# COLUMN COUNT: 19

SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE = spark.sql(f"""SELECT
WM_LABOR_TRAN_DTL_CRIT_PRE.DC_NBR,
WM_LABOR_TRAN_DTL_CRIT_PRE.LABOR_TRAN_DTL_CRIT_ID,
WM_LABOR_TRAN_DTL_CRIT_PRE.LABOR_TRAN_DTL_ID,
WM_LABOR_TRAN_DTL_CRIT_PRE.TRAN_NBR,
WM_LABOR_TRAN_DTL_CRIT_PRE.CRIT_SEQ_NBR,
WM_LABOR_TRAN_DTL_CRIT_PRE.CRIT_TYPE,
WM_LABOR_TRAN_DTL_CRIT_PRE.CRIT_VAL,
WM_LABOR_TRAN_DTL_CRIT_PRE.CREATED_SOURCE_TYPE,
WM_LABOR_TRAN_DTL_CRIT_PRE.CREATED_SOURCE,
WM_LABOR_TRAN_DTL_CRIT_PRE.CREATED_DTTM,
WM_LABOR_TRAN_DTL_CRIT_PRE.LAST_UPDATED_SOURCE_TYPE,
WM_LABOR_TRAN_DTL_CRIT_PRE.LAST_UPDATED_SOURCE,
WM_LABOR_TRAN_DTL_CRIT_PRE.LAST_UPDATED_DTTM,
WM_LABOR_TRAN_DTL_CRIT_PRE.WHSE,
WM_LABOR_TRAN_DTL_CRIT_PRE.MISC_TXT_1,
WM_LABOR_TRAN_DTL_CRIT_PRE.MISC_TXT_2,
WM_LABOR_TRAN_DTL_CRIT_PRE.MISC_NUM_1,
WM_LABOR_TRAN_DTL_CRIT_PRE.MISC_NUM_2,
WM_LABOR_TRAN_DTL_CRIT_PRE.HIBERNATE_VERSION
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE_temp = SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE.toDF(*["SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___" + col for col in SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___LABOR_TRAN_DTL_CRIT_ID as LABOR_TRAN_DTL_CRIT_ID", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___LABOR_TRAN_DTL_ID as LABOR_TRAN_DTL_ID", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___TRAN_NBR as TRAN_NBR", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___CRIT_SEQ_NBR as CRIT_SEQ_NBR", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___CRIT_TYPE as CRIT_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___CRIT_VAL as CRIT_VAL", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_PRE___HIBERNATE_VERSION as HIBERNATE_VERSION" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 21

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LABOR_TRAN_DTL_CRIT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_temp = SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT.toDF(*["SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___" + col for col in SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT.columns])

JNR_WM_LABOR_TRAN_DTL_CRIT = SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_temp.SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT_temp.SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___WM_LABOR_TRAN_DTL_CRIT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LABOR_TRAN_DTL_CRIT_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___LABOR_TRAN_DTL_CRIT_ID as LABOR_TRAN_DTL_CRIT_ID", \
	"JNR_SITE_PROFILE___LABOR_TRAN_DTL_ID as LABOR_TRAN_DTL_ID", \
	"JNR_SITE_PROFILE___TRAN_NBR as TRAN_NBR", \
	"JNR_SITE_PROFILE___CRIT_SEQ_NBR as CRIT_SEQ_NBR", \
	"JNR_SITE_PROFILE___CRIT_TYPE as CRIT_TYPE", \
	"JNR_SITE_PROFILE___CRIT_VAL as CRIT_VAL", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___WM_LABOR_TRAN_DTL_CRIT_ID as i_WM_LABOR_TRAN_DTL_CRIT_ID", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_LABOR_TRAN_DTL_CRIT___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LABOR_TRAN_DTL_CRIT_temp = JNR_WM_LABOR_TRAN_DTL_CRIT.toDF(*["JNR_WM_LABOR_TRAN_DTL_CRIT___" + col for col in JNR_WM_LABOR_TRAN_DTL_CRIT.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_LABOR_TRAN_DTL_CRIT_temp.selectExpr( \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___LABOR_TRAN_DTL_CRIT_ID as LABOR_TRAN_DTL_CRIT_ID", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___LABOR_TRAN_DTL_ID as LABOR_TRAN_DTL_ID", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___TRAN_NBR as TRAN_NBR", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___CRIT_SEQ_NBR as CRIT_SEQ_NBR", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___CRIT_TYPE as CRIT_TYPE", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___CRIT_VAL as CRIT_VAL", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___WHSE as WHSE", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___i_WM_LABOR_TRAN_DTL_CRIT_ID as i_WM_LABOR_TRAN_DTL_CRIT_ID", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_LABOR_TRAN_DTL_CRIT___i_LOAD_TSTMP as i_LOAD_TSTMP")\
    .filter("i_WM_LABOR_TRAN_DTL_CRIT_ID is Null OR (  i_WM_LABOR_TRAN_DTL_CRIT_ID is NOT Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01') ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___LABOR_TRAN_DTL_CRIT_ID as LABOR_TRAN_DTL_CRIT_ID", \
	"FIL_UNCHANGED_RECORDS___LABOR_TRAN_DTL_ID as LABOR_TRAN_DTL_ID", \
	"FIL_UNCHANGED_RECORDS___TRAN_NBR as TRAN_NBR", \
	"FIL_UNCHANGED_RECORDS___CRIT_SEQ_NBR as CRIT_SEQ_NBR", \
	"FIL_UNCHANGED_RECORDS___CRIT_TYPE as CRIT_TYPE", \
	"FIL_UNCHANGED_RECORDS___CRIT_VAL as CRIT_VAL", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___WHSE as WHSE", \
	"FIL_UNCHANGED_RECORDS___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_UNCHANGED_RECORDS___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_UNCHANGED_RECORDS___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_UNCHANGED_RECORDS___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_UNCHANGED_RECORDS___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_WM_LABOR_TRAN_DTL_CRIT_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___LABOR_TRAN_DTL_CRIT_ID as LABOR_TRAN_DTL_CRIT_ID", \
	"EXP_UPD_VALIDATOR___LABOR_TRAN_DTL_ID as LABOR_TRAN_DTL_ID", \
	"EXP_UPD_VALIDATOR___TRAN_NBR as TRAN_NBR", \
	"EXP_UPD_VALIDATOR___CRIT_SEQ_NBR as CRIT_SEQ_NBR", \
	"EXP_UPD_VALIDATOR___CRIT_TYPE as CRIT_TYPE", \
	"EXP_UPD_VALIDATOR___CRIT_VAL as CRIT_VAL", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___WHSE as WHSE", \
	"EXP_UPD_VALIDATOR___MISC_TXT_1 as MISC_TXT_1", \
	"EXP_UPD_VALIDATOR___MISC_TXT_2 as MISC_TXT_2", \
	"EXP_UPD_VALIDATOR___MISC_NUM_1 as MISC_NUM_1", \
	"EXP_UPD_VALIDATOR___MISC_NUM_2 as MISC_NUM_2", \
	"EXP_UPD_VALIDATOR___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LABOR_TRAN_DTL_CRIT1, type TARGET 
# COLUMN COUNT: 21

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LABOR_TRAN_DTL_CRIT_ID = target.WM_LABOR_TRAN_DTL_CRIT_ID"""
#   refined_perf_table = "WM_LABOR_TRAN_DTL_CRIT"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LABOR_TRAN_DTL_CRIT", "WM_LABOR_TRAN_DTL_CRIT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LABOR_TRAN_DTL_CRIT", "WM_LABOR_TRAN_DTL_CRIT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	