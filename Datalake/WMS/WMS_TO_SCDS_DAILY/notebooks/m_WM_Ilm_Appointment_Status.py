#Code converted on 2023-06-21 15:27:57
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from utils.logger import *

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

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE = spark.sql(f"""SELECT
WM_ILM_APPOINTMENT_STATUS_PRE.DC_NBR,
WM_ILM_APPOINTMENT_STATUS_PRE.APPT_STATUS_CODE,
WM_ILM_APPOINTMENT_STATUS_PRE.DESCRIPTION,
WM_ILM_APPOINTMENT_STATUS_PRE.CREATED_DTTM,
WM_ILM_APPOINTMENT_STATUS_PRE.LAST_UPDATED_DTTM
FROM WM_ILM_APPOINTMENT_STATUS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS = spark.sql(f"""SELECT
WM_ILM_APPOINTMENT_STATUS.LOCATION_ID,
WM_ILM_APPOINTMENT_STATUS.WM_ILM_APPT_STATUS_CD,
WM_ILM_APPOINTMENT_STATUS.WM_CREATED_TSTMP,
WM_ILM_APPOINTMENT_STATUS.WM_LAST_UPDATED_TSTMP,
WM_ILM_APPOINTMENT_STATUS.LOAD_TSTMP
FROM WM_ILM_APPOINTMENT_STATUS
WHERE WM_ILM_APPT_STATUS_CD IN (SELECT APPT_STATUS_CODE FROM WM_ILM_APPOINTMENT_STATUS_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___APPT_STATUS_CODE as APPT_STATUS_CODE", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 7

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_ILM_APPOINTMENT_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_ILM_APPOINTMENT_STATUS = SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___WM_ILM_APPT_STATUS_CD == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___APPT_STATUS_CODE],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___APPT_STATUS_CODE as APPT_STATUS_CODE", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___LOCATION_ID as i_LOCATION_ID1", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___WM_ILM_APPT_STATUS_CD as i_WM_ILM_APPT_STATUS_CD", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_STATUS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ILM_APPOINTMENT_STATUS_temp = JNR_WM_ILM_APPOINTMENT_STATUS.toDF(*["JNR_WM_ILM_APPOINTMENT_STATUS___" + col for col in JNR_WM_ILM_APPOINTMENT_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_ILM_APPOINTMENT_STATUS_temp.selectExpr( \
	"JNR_WM_ILM_APPOINTMENT_STATUS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___APPT_STATUS_CODE as APPT_STATUS_CODE", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___i_WM_ILM_APPT_STATUS_CD as i_WM_ILM_APPT_STATUS_CD", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_ILM_APPOINTMENT_STATUS___i_LOAD_TSTMP as i_LOAD_TSTMP")\
    .filter("i_WM_ILM_APPT_STATUS_CD is Null OR ( i_WM_ILM_APPT_STATUS_CD is not Null AND\
    ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(in_WM_CREATED_TSTMP, date'1900-01-01') \
    OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01') ) )").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___APPT_STATUS_CODE as APPT_STATUS_CODE", \
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_WM_ILM_APPT_STATUS_CD IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( \
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_OUTPUT_VALIDATOR___APPT_STATUS_CODE as APPT_STATUS_CODE", \
	"EXP_OUTPUT_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_OUTPUT_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_OUTPUT_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)) .when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ILM_APPOINTMENT_STATUS1, type TARGET 
# COLUMN COUNT: 7

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ILM_APPT_STATUS_CD = target.WM_ILM_APPT_STATUS_CD"""
  refined_perf_table = "WM_ILM_APPOINTMENT_STATUS"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ILM_APPOINTMENT_STATUS", "WM_ILM_APPOINTMENT_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ILM_APPOINTMENT_STATUS", "WM_ILM_APPOINTMENT_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	