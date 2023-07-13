#Code converted on 2023-06-22 20:58:44
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
refined_perf_table = f"{refine}.WM_SEC_USER"
raw_perf_table = f"{raw}.WM_SEC_USER_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SEC_USER_PRE, type SOURCE 
# COLUMN COUNT: 21

SQ_Shortcut_to_WM_SEC_USER_PRE = spark.sql(f"""SELECT
DC_NBR,
SEC_USER_ID,
LOGIN_USER_ID,
USER_NAME,
USER_DESC,
PSWD,
PSWD_EXP_DATE,
PSWD_CHANGE_AT_LOGIN,
CAN_CHNG_PSWD,
DISABLED,
LOCKED_OUT,
LAST_LOGIN,
GRACE_LOGINS,
LOCKED_OUT_EXPIRATION,
FAILED_LOGIN_ATTEMPTS,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
SEC_POLICY_SET_ID,
WM_VERSION_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SEC_USER, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_SEC_USER = spark.sql(f"""SELECT
LOCATION_ID,
WM_SEC_USER_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_SEC_USER_ID IN (SELECT SEC_USER_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SEC_USER_PRE_temp = SQ_Shortcut_to_WM_SEC_USER_PRE.toDF(*["SQ_Shortcut_to_WM_SEC_USER_PRE___" + col for col in SQ_Shortcut_to_WM_SEC_USER_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_SEC_USER_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_SEC_USER_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___SEC_USER_ID as SEC_USER_ID", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___LOGIN_USER_ID as LOGIN_USER_ID", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___USER_NAME as USER_NAME", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___USER_DESC as USER_DESC", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___PSWD as PSWD", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___PSWD_EXP_DATE as PSWD_EXP_DATE", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___PSWD_CHANGE_AT_LOGIN as PSWD_CHANGE_AT_LOGIN", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___CAN_CHNG_PSWD as CAN_CHNG_PSWD", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___DISABLED as DISABLED", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___LOCKED_OUT as LOCKED_OUT", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___LAST_LOGIN as LAST_LOGIN", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___GRACE_LOGINS as GRACE_LOGINS", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___LOCKED_OUT_EXPIRATION as LOCKED_OUT_EXPIRATION", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___FAILED_LOGIN_ATTEMPTS as FAILED_LOGIN_ATTEMPTS", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___USER_ID as USER_ID", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___SEC_POLICY_SET_ID as SEC_POLICY_SET_ID", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___WM_VERSION_ID as WM_VERSION_ID", 
	"SQ_Shortcut_to_WM_SEC_USER_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 22

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_SEC_USER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 25

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_SEC_USER_temp = SQ_Shortcut_to_WM_SEC_USER.toDF(*["SQ_Shortcut_to_WM_SEC_USER___" + col for col in SQ_Shortcut_to_WM_SEC_USER.columns])

JNR_WM_SEC_USER = SQ_Shortcut_to_WM_SEC_USER_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_SEC_USER_temp.SQ_Shortcut_to_WM_SEC_USER___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_SEC_USER_temp.SQ_Shortcut_to_WM_SEC_USER___WM_SEC_USER_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SEC_USER_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___SEC_USER_ID as SEC_USER_ID", 
	"JNR_SITE_PROFILE___LOGIN_USER_ID as LOGIN_USER_ID", 
	"JNR_SITE_PROFILE___USER_NAME as USER_NAME", 
	"JNR_SITE_PROFILE___USER_DESC as USER_DESC", 
	"JNR_SITE_PROFILE___PSWD as PSWD", 
	"JNR_SITE_PROFILE___PSWD_EXP_DATE as PSWD_EXP_DATE", 
	"JNR_SITE_PROFILE___PSWD_CHANGE_AT_LOGIN as PSWD_CHANGE_AT_LOGIN", 
	"JNR_SITE_PROFILE___CAN_CHNG_PSWD as CAN_CHNG_PSWD", 
	"JNR_SITE_PROFILE___DISABLED as DISABLED", 
	"JNR_SITE_PROFILE___LOCKED_OUT as LOCKED_OUT", 
	"JNR_SITE_PROFILE___LAST_LOGIN as LAST_LOGIN", 
	"JNR_SITE_PROFILE___GRACE_LOGINS as GRACE_LOGINS", 
	"JNR_SITE_PROFILE___LOCKED_OUT_EXPIRATION as LOCKED_OUT_EXPIRATION", 
	"JNR_SITE_PROFILE___FAILED_LOGIN_ATTEMPTS as FAILED_LOGIN_ATTEMPTS", 
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_SITE_PROFILE___USER_ID as USER_ID", 
	"JNR_SITE_PROFILE___SEC_POLICY_SET_ID as SEC_POLICY_SET_ID", 
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", 
	"SQ_Shortcut_to_WM_SEC_USER___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_SEC_USER___WM_SEC_USER_ID as i_WM_SEC_USER_ID", 
	"SQ_Shortcut_to_WM_SEC_USER___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"SQ_Shortcut_to_WM_SEC_USER___WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"SQ_Shortcut_to_WM_SEC_USER___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_SEC_USER_temp = JNR_WM_SEC_USER.toDF(*["JNR_WM_SEC_USER___" + col for col in JNR_WM_SEC_USER.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_SEC_USER_temp.selectExpr( 
	"JNR_WM_SEC_USER___i_WM_SEC_USER_ID as i_WM_SEC_USER_ID", 
	"JNR_WM_SEC_USER___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"JNR_WM_SEC_USER___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"JNR_WM_SEC_USER___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"JNR_WM_SEC_USER___LOCATION_ID as LOCATION_ID1", 
	"JNR_WM_SEC_USER___SEC_USER_ID as SEC_USER_ID", 
	"JNR_WM_SEC_USER___LOGIN_USER_ID as LOGIN_USER_ID", 
	"JNR_WM_SEC_USER___USER_NAME as USER_NAME", 
	"JNR_WM_SEC_USER___USER_DESC as USER_DESC", 
	"JNR_WM_SEC_USER___PSWD as PSWD", 
	"JNR_WM_SEC_USER___PSWD_EXP_DATE as PSWD_EXP_DATE", 
	"JNR_WM_SEC_USER___PSWD_CHANGE_AT_LOGIN as PSWD_CHANGE_AT_LOGIN", 
	"JNR_WM_SEC_USER___CAN_CHNG_PSWD as CAN_CHNG_PSWD", 
	"JNR_WM_SEC_USER___DISABLED as DISABLED", 
	"JNR_WM_SEC_USER___LOCKED_OUT as LOCKED_OUT", 
	"JNR_WM_SEC_USER___LAST_LOGIN as LAST_LOGIN", 
	"JNR_WM_SEC_USER___GRACE_LOGINS as GRACE_LOGINS1", 
	"JNR_WM_SEC_USER___LOCKED_OUT_EXPIRATION as LOCKED_OUT_EXPIRATION", 
	"JNR_WM_SEC_USER___FAILED_LOGIN_ATTEMPTS as FAILED_LOGIN_ATTEMPTS1", 
	"JNR_WM_SEC_USER___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_WM_SEC_USER___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_WM_SEC_USER___USER_ID as USER_ID", 
	"JNR_WM_SEC_USER___SEC_POLICY_SET_ID as SEC_POLICY_SET_ID", 
	"JNR_WM_SEC_USER___WM_VERSION_ID as WM_VERSION_ID1").filter(expr("i_WM_SEC_USER_ID IS NULL OR (NOT i_WM_SEC_USER_ID IS NULL AND (COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01')) OR (COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___SEC_USER_ID as SEC_USER_ID", 
	"FIL_UNCHANGED_RECORDS___LOGIN_USER_ID as LOGIN_USER_ID", 
	"FIL_UNCHANGED_RECORDS___USER_NAME as USER_NAME", 
	"FIL_UNCHANGED_RECORDS___USER_DESC as USER_DESC", 
	"FIL_UNCHANGED_RECORDS___PSWD as PSWD", 
	"FIL_UNCHANGED_RECORDS___PSWD_EXP_DATE as PSWD_EXP_DATE", 
	"FIL_UNCHANGED_RECORDS___PSWD_CHANGE_AT_LOGIN as PSWD_CHANGE_AT_LOGIN", 
	"FIL_UNCHANGED_RECORDS___CAN_CHNG_PSWD as CAN_CHNG_PSWD", 
	"FIL_UNCHANGED_RECORDS___DISABLED as DISABLED", 
	"FIL_UNCHANGED_RECORDS___LOCKED_OUT as LOCKED_OUT", 
	"FIL_UNCHANGED_RECORDS___LAST_LOGIN as LAST_LOGIN", 
	"FIL_UNCHANGED_RECORDS___GRACE_LOGINS1 as GRACE_LOGINS1", 
	"FIL_UNCHANGED_RECORDS___LOCKED_OUT_EXPIRATION as LOCKED_OUT_EXPIRATION", 
	"FIL_UNCHANGED_RECORDS___FAILED_LOGIN_ATTEMPTS1 as FAILED_LOGIN_ATTEMPTS1", 
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", 
	"FIL_UNCHANGED_RECORDS___SEC_POLICY_SET_ID as SEC_POLICY_SET_ID", 
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID1 as WM_VERSION_ID1", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_SEC_USER_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID1 as LOCATION_ID1", 
	"EXP_UPD_VALIDATOR___SEC_USER_ID as SEC_USER_ID", 
	"EXP_UPD_VALIDATOR___LOGIN_USER_ID as LOGIN_USER_ID", 
	"EXP_UPD_VALIDATOR___USER_NAME as USER_NAME", 
	"EXP_UPD_VALIDATOR___USER_DESC as USER_DESC", 
	"EXP_UPD_VALIDATOR___PSWD as PSWD", 
	"EXP_UPD_VALIDATOR___PSWD_EXP_DATE as PSWD_EXP_DATE", 
	"EXP_UPD_VALIDATOR___PSWD_CHANGE_AT_LOGIN as PSWD_CHANGE_AT_LOGIN", 
	"EXP_UPD_VALIDATOR___CAN_CHNG_PSWD as CAN_CHNG_PSWD", 
	"EXP_UPD_VALIDATOR___DISABLED as DISABLED", 
	"EXP_UPD_VALIDATOR___LOCKED_OUT as LOCKED_OUT", 
	"EXP_UPD_VALIDATOR___LAST_LOGIN as LAST_LOGIN", 
	"EXP_UPD_VALIDATOR___GRACE_LOGINS1 as GRACE_LOGINS1", 
	"EXP_UPD_VALIDATOR___LOCKED_OUT_EXPIRATION as LOCKED_OUT_EXPIRATION", 
	"EXP_UPD_VALIDATOR___FAILED_LOGIN_ATTEMPTS1 as FAILED_LOGIN_ATTEMPTS1", 
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"EXP_UPD_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", 
	"EXP_UPD_VALIDATOR___USER_ID as USER_ID", 
	"EXP_UPD_VALIDATOR___SEC_POLICY_SET_ID as SEC_POLICY_SET_ID", 
	"EXP_UPD_VALIDATOR___WM_VERSION_ID1 as WM_VERSION_ID1", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)),lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_SEC_USER, type TARGET 
# COLUMN COUNT: 22


Shortcut_to_WM_SEC_USER = UPD_INS_UPD.selectExpr( 
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID", 
	"CAST(SEC_USER_ID AS BIGINT) as WM_SEC_USER_ID", 
	"CAST(LOGIN_USER_ID AS STRING) as WM_LOGIN_USER_ID", 
	"CAST(USER_NAME AS STRING) as WM_USER_NAME", 
	"CAST(USER_DESC AS STRING) as WM_USER_DESC", 
	"CAST(SEC_POLICY_SET_ID AS BIGINT) as WM_SEC_POLICY_SET_ID", 
	"CAST(PSWD AS STRING) as WM_PSWD", 
	"CAST(PSWD_EXP_DATE AS DATE) as WM_PSWD_EXP_DT", 
	"CAST(PSWD_CHANGE_AT_LOGIN AS STRING) as PSWD_CHANGE_AT_LOGIN_FLAG", 
	"CAST(CAN_CHNG_PSWD AS STRING) as CAN_CHNG_PSWD_FLAG", 
	"CAST(DISABLED AS STRING) as DISABLED_FLAG", 
	"CAST(LOCKED_OUT AS STRING) as LOCKED_OUT_FLAG", 
	"CAST(LAST_LOGIN AS TIMESTAMP) as LAST_LOGIN_TSTMP", 
	"CAST(GRACE_LOGINS1 AS BIGINT) as GRACE_LOGINS", 
	"CAST(LOCKED_OUT_EXPIRATION AS TIMESTAMP) as LOCKED_OUT_EXP_TSTMP", 
	"CAST(FAILED_LOGIN_ATTEMPTS1 AS BIGINT) as FAILED_LOGIN_ATTEMPTS", 
	"CAST(USER_ID AS STRING) as WM_USER_ID", 
	"CAST(WM_VERSION_ID1 AS BIGINT) as WM_VERSION_ID", 
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP", 
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SEC_USER_ID = target.WM_SEC_USER_ID"""
  # refined_perf_table = "WM_SEC_USER"
  executeMerge(Shortcut_to_WM_SEC_USER, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_SEC_USER", "WM_SEC_USER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_SEC_USER", "WM_SEC_USER","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	