#Code converted on 2023-06-21 15:27:55
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

refined_perf_table = f"{refine}.WM_ILM_APPOINTMENT_TYPE"
raw_perf_table = f"{raw}.WM_ILM_APPOINTMENT_TYPE_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE = spark.sql(f"""SELECT
WM_ILM_APPOINTMENT_TYPE.LOCATION_ID,
WM_ILM_APPOINTMENT_TYPE.WM_APPT_TYPE_ID,
WM_ILM_APPOINTMENT_TYPE.WM_APPT_TYPE_DESC,
WM_ILM_APPOINTMENT_TYPE.WM_CREATED_TSTMP,
WM_ILM_APPOINTMENT_TYPE.WM_LAST_UPDATED_TSTMP,
WM_ILM_APPOINTMENT_TYPE.LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_APPT_TYPE_ID IN ( SELECT APPT_TYPE FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE = spark.sql(f"""SELECT
WM_ILM_APPOINTMENT_TYPE_PRE.DC_NBR,
WM_ILM_APPOINTMENT_TYPE_PRE.APPT_TYPE,
WM_ILM_APPOINTMENT_TYPE_PRE.DESCRIPTION,
WM_ILM_APPOINTMENT_TYPE_PRE.CREATED_DTTM,
WM_ILM_APPOINTMENT_TYPE_PRE.LAST_UPDATED_DTTM,
WM_ILM_APPOINTMENT_TYPE_PRE.LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___APPT_TYPE as APPT_TYPE", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_PRE___LOAD_TSTMP as LOAD_TSTMP" \
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
# COLUMN COUNT: 8

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_ILM_APPOINTMENT_TYPE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_ILM_APPOINTMENT_TYPE = SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___WM_APPT_TYPE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___APPT_TYPE],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___APPT_TYPE as APPT_TYPE", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___WM_APPT_TYPE_ID as WM_APPT_TYPE_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___WM_APPT_TYPE_DESC as WM_APPT_TYPE_DESC", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_TYPE___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ILM_APPOINTMENT_TYPE_temp = JNR_WM_ILM_APPOINTMENT_TYPE.toDF(*["JNR_WM_ILM_APPOINTMENT_TYPE___" + col for col in JNR_WM_ILM_APPOINTMENT_TYPE.columns])

FIL_UNCHANGED_REC = JNR_WM_ILM_APPOINTMENT_TYPE_temp.selectExpr( \
	"JNR_WM_ILM_APPOINTMENT_TYPE___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___APPT_TYPE as APPT_TYPE", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___WM_APPT_TYPE_ID as WM_APPT_TYPE_ID", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___WM_APPT_TYPE_DESC as WM_APPT_TYPE_DESC", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_ILM_APPOINTMENT_TYPE___in_LOAD_TSTMP as in_LOAD_TSTMP")\
    .filter("WM_APPT_TYPE_ID is Null OR ( WM_APPT_TYPE_ID is not Null AND\
     ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01') \
     OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___APPT_TYPE as APPT_TYPE", \
	"FIL_UNCHANGED_REC___DESCRIPTION as DESCRIPTION", \
	"FIL_UNCHANGED_REC___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_APPT_TYPE_ID as WM_APPT_TYPE_ID", \
	"FIL_UNCHANGED_REC___WM_APPT_TYPE_DESC as WM_APPT_TYPE_DESC", \
	"FIL_UNCHANGED_REC___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_REC___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP (), FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTP_exp", \
	"IF (FIL_UNCHANGED_REC___WM_APPT_TYPE_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___APPT_TYPE as APPT_TYPE", \
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTP_exp as LOAD_TSTP_exp", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(1)) , lit(0)).when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ILM_APPOINTMENT_TYPE1, type TARGET 
# COLUMN COUNT: 7

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_APPT_TYPE_ID = target.WM_APPT_TYPE_ID"""
#   refined_perf_table = "WM_ILM_APPOINTMENT_TYPE"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ILM_APPOINTMENT_TYPE", "WM_ILM_APPOINTMENT_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ILM_APPOINTMENT_TYPE", "WM_ILM_APPOINTMENT_TYPE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	