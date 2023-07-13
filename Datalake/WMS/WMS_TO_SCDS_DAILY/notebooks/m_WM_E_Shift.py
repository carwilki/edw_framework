#Code converted on 2023-06-21 15:26:45
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
# args = parser.parse_args()
# env = args.env
env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
raw_perf_table = f"{raw}.WM_E_SHIFT_PRE"
refined_perf_table = f"{refine}.WM_E_SHIFT"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic= ' -- '  #args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_SHIFT_PRE1, type SOURCE 
# COLUMN COUNT: 25

SQ_Shortcut_to_WM_E_SHIFT_PRE1 = spark.sql(f"""SELECT
DC_NBR,
SHIFT_ID,
EFF_DATE,
SHIFT_CODE,
DESCRIPTION,
OT_INDIC,
OT_PAY_FCT,
WEEK_MIN_OT_HRS,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
WHSE,
REPORT_SHIFT,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
VERSION_ID,
ROT_SHIFT_IND,
ROT_SHIFT_NUM_DAYS,
ROT_SHIFT_START_DATE,
SCHED_SHIFT,
CREATED_DTTM,
LAST_UPDATED_DTTM,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_SHIFT, type SOURCE 
# COLUMN COUNT: 26

SQ_Shortcut_to_WM_E_SHIFT = spark.sql(f"""SELECT
LOCATION_ID,
WM_SHIFT_ID,
WM_SHIFT_CD,
WM_SHIFT_DESC,
WM_WHSE,
WM_SHIFT_EFF_DT,
OT_FLAG,
OT_PAY_FCT,
WEEK_MIN_OT_HRS,
SCHED_SHIFT,
REPORT_SHIFT,
ROT_SHIFT_FLAG,
ROT_SHIFT_NUM_DAYS,
ROT_SHIFT_START_TSTMP,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
WM_USER_ID,
WM_VERSION_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 25

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_SHIFT_PRE1_temp = SQ_Shortcut_to_WM_E_SHIFT_PRE1.toDF(*["SQ_Shortcut_to_WM_E_SHIFT_PRE1___" + col for col in SQ_Shortcut_to_WM_E_SHIFT_PRE1.columns])

EXPTRANS = SQ_Shortcut_to_WM_E_SHIFT_PRE1_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_SHIFT_PRE1___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___SHIFT_ID as SHIFT_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___EFF_DATE as EFF_DATE", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___SHIFT_CODE as SHIFT_CODE", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___OT_INDIC as OT_INDIC", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___OT_PAY_FCT as OT_PAY_FCT", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___REPORT_SHIFT as REPORT_SHIFT", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___ROT_SHIFT_IND as ROT_SHIFT_IND", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___SCHED_SHIFT as SCHED_SHIFT", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_SHIFT_PRE1___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 27

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_SHIFT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 50

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_SHIFT_temp = SQ_Shortcut_to_WM_E_SHIFT.toDF(*["SQ_Shortcut_to_WM_E_SHIFT___" + col for col in SQ_Shortcut_to_WM_E_SHIFT.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_E_SHIFT = SQ_Shortcut_to_WM_E_SHIFT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_SHIFT_temp.SQ_Shortcut_to_WM_E_SHIFT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_SHIFT_temp.SQ_Shortcut_to_WM_E_SHIFT___WM_SHIFT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SHIFT_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___SHIFT_ID as SHIFT_ID", \
	"JNR_SITE_PROFILE___EFF_DATE as EFF_DATE", \
	"JNR_SITE_PROFILE___SHIFT_CODE as SHIFT_CODE", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___OT_INDIC as OT_INDIC", \
	"JNR_SITE_PROFILE___OT_PAY_FCT as OT_PAY_FCT", \
	"JNR_SITE_PROFILE___WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___REPORT_SHIFT as REPORT_SHIFT", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___ROT_SHIFT_IND as ROT_SHIFT_IND", \
	"JNR_SITE_PROFILE___ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS", \
	"JNR_SITE_PROFILE___ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE", \
	"JNR_SITE_PROFILE___SCHED_SHIFT as SCHED_SHIFT", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_SHIFT___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_SHIFT_ID as WM_SHIFT_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_SHIFT_CD as WM_SHIFT_CD", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_SHIFT_DESC as WM_SHIFT_DESC", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_WHSE as WM_WHSE", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_SHIFT_EFF_DT as WM_SHIFT_EFF_DT", \
	"SQ_Shortcut_to_WM_E_SHIFT___OT_FLAG as OT_FLAG", \
	"SQ_Shortcut_to_WM_E_SHIFT___OT_PAY_FCT as OT_PAY_FCT1", \
	"SQ_Shortcut_to_WM_E_SHIFT___WEEK_MIN_OT_HRS as in_WEEK_MIN_OT_HRS", \
	"SQ_Shortcut_to_WM_E_SHIFT___SCHED_SHIFT as in_SCHED_SHIFT", \
	"SQ_Shortcut_to_WM_E_SHIFT___REPORT_SHIFT as in_REPORT_SHIFT", \
	"SQ_Shortcut_to_WM_E_SHIFT___ROT_SHIFT_FLAG as ROT_SHIFT_FLAG", \
	"SQ_Shortcut_to_WM_E_SHIFT___ROT_SHIFT_NUM_DAYS as in_ROT_SHIFT_NUM_DAYS", \
	"SQ_Shortcut_to_WM_E_SHIFT___ROT_SHIFT_START_TSTMP as ROT_SHIFT_START_TSTMP", \
	"SQ_Shortcut_to_WM_E_SHIFT___MISC_TXT_1 as in_MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_SHIFT___MISC_TXT_2 as in_MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_SHIFT___MISC_NUM_1 as in_MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_SHIFT___MISC_NUM_2 as in_MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_USER_ID as WM_USER_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_VERSION_ID as WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_E_SHIFT___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_E_SHIFT___DELETE_FLAG as DELETE_FLAG", \
	"SQ_Shortcut_to_WM_E_SHIFT___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 50

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_SHIFT_temp = JNR_WM_E_SHIFT.toDF(*["JNR_WM_E_SHIFT___" + col for col in JNR_WM_E_SHIFT.columns])

FIL_UNCHANGED_REC = JNR_WM_E_SHIFT_temp.selectExpr( \
	"JNR_WM_E_SHIFT___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_SHIFT___SHIFT_ID as SHIFT_ID", \
	"JNR_WM_E_SHIFT___EFF_DATE as EFF_DATE", \
	"JNR_WM_E_SHIFT___SHIFT_CODE as SHIFT_CODE", \
	"JNR_WM_E_SHIFT___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_E_SHIFT___OT_INDIC as OT_INDIC", \
	"JNR_WM_E_SHIFT___OT_PAY_FCT as OT_PAY_FCT", \
	"JNR_WM_E_SHIFT___WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS", \
	"JNR_WM_E_SHIFT___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_SHIFT___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_SHIFT___USER_ID as USER_ID", \
	"JNR_WM_E_SHIFT___WHSE as WHSE", \
	"JNR_WM_E_SHIFT___REPORT_SHIFT as REPORT_SHIFT", \
	"JNR_WM_E_SHIFT___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_SHIFT___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_SHIFT___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_SHIFT___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_SHIFT___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_SHIFT___ROT_SHIFT_IND as ROT_SHIFT_IND", \
	"JNR_WM_E_SHIFT___ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS", \
	"JNR_WM_E_SHIFT___ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE", \
	"JNR_WM_E_SHIFT___SCHED_SHIFT as SCHED_SHIFT", \
	"JNR_WM_E_SHIFT___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_E_SHIFT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_E_SHIFT___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_E_SHIFT___WM_SHIFT_ID as WM_SHIFT_ID", \
	"JNR_WM_E_SHIFT___WM_SHIFT_CD as WM_SHIFT_CD", \
	"JNR_WM_E_SHIFT___WM_SHIFT_DESC as WM_SHIFT_DESC", \
	"JNR_WM_E_SHIFT___WM_WHSE as WM_WHSE", \
	"JNR_WM_E_SHIFT___WM_SHIFT_EFF_DT as WM_SHIFT_EFF_DT", \
	"JNR_WM_E_SHIFT___OT_FLAG as OT_FLAG", \
	"JNR_WM_E_SHIFT___OT_PAY_FCT1 as OT_PAY_FCT1", \
	"JNR_WM_E_SHIFT___in_WEEK_MIN_OT_HRS as in_WEEK_MIN_OT_HRS", \
	"JNR_WM_E_SHIFT___in_SCHED_SHIFT as in_SCHED_SHIFT", \
	"JNR_WM_E_SHIFT___in_REPORT_SHIFT as in_REPORT_SHIFT", \
	"JNR_WM_E_SHIFT___ROT_SHIFT_FLAG as ROT_SHIFT_FLAG", \
	"JNR_WM_E_SHIFT___in_ROT_SHIFT_NUM_DAYS as in_ROT_SHIFT_NUM_DAYS", \
	"JNR_WM_E_SHIFT___ROT_SHIFT_START_TSTMP as ROT_SHIFT_START_TSTMP", \
	"JNR_WM_E_SHIFT___in_MISC_TXT_1 as in_MISC_TXT_1", \
	"JNR_WM_E_SHIFT___in_MISC_TXT_2 as in_MISC_TXT_2", \
	"JNR_WM_E_SHIFT___in_MISC_NUM_1 as in_MISC_NUM_1", \
	"JNR_WM_E_SHIFT___in_MISC_NUM_2 as in_MISC_NUM_2", \
	"JNR_WM_E_SHIFT___WM_USER_ID as WM_USER_ID", \
	"JNR_WM_E_SHIFT___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_WM_E_SHIFT___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"JNR_WM_E_SHIFT___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"JNR_WM_E_SHIFT___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_E_SHIFT___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_E_SHIFT___DELETE_FLAG as DELETE_FLAG", \
	"JNR_WM_E_SHIFT___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_SHIFT_ID is Null OR SHIFT_ID is Null OR (  WM_SHIFT_ID is NOT Null AND \
     ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(WM_CREATE_TSTMP, date'1900-01-01') \
    OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(WM_MOD_TSTMP, date'1900-01-01') OR\
    COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 54

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns]) \
.withColumn("v_CREATE_DATE_TIME", expr("""IF(CREATE_DATE_TIME IS NULL, date'1900-01-01', CREATE_DATE_TIME)""")) \
	.withColumn("v_MOD_DATE_TIME", expr("""IF(MOD_DATE_TIME IS NULL, date'1900-01-01', MOD_DATE_TIME)""")) \
	.withColumn("v_CREATED_DTTM", expr("""IF(CREATED_DTTM IS NULL, date'1900-01-01', CREATED_DTTM)""")) \
	.withColumn("v_LAST_UPDATED_DTTM", expr("""IF(LAST_UPDATED_DTTM IS NULL, date'1900-01-01', LAST_UPDATED_DTTM)""")) \
	.withColumn("v_WM_CREATE_TSTMP", expr("""IF(WM_CREATE_TSTMP IS NULL, date'1900-01-01', WM_CREATE_TSTMP)""")) \
	.withColumn("v_WM_MOD_TSTMP", expr("""IF(WM_MOD_TSTMP IS NULL, date'1900-01-01', WM_MOD_TSTMP)""")) \
	.withColumn("v_WM_CREATED_TSTMP", expr("""IF(WM_CREATED_TSTMP IS NULL, date'1900-01-01', WM_CREATED_TSTMP)""")) \
	.withColumn("v_WM_LAST_UPDATED_TSTMP", expr("""IF(WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', WM_LAST_UPDATED_TSTMP)"""))

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___SHIFT_ID as SHIFT_ID", \
	"FIL_UNCHANGED_REC___EFF_DATE as EFF_DATE", \
	"FIL_UNCHANGED_REC___SHIFT_CODE as SHIFT_CODE", \
	"FIL_UNCHANGED_REC___DESCRIPTION as DESCRIPTION", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_REC___OT_INDIC)) IN ('Y', '1') THEN '1' ELSE '0' END as OT_INDIC_EXP", \
	"FIL_UNCHANGED_REC___OT_PAY_FCT as OT_PAY_FCT", \
	"FIL_UNCHANGED_REC___WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS", \
	"FIL_UNCHANGED_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_REC___USER_ID as USER_ID", \
	"FIL_UNCHANGED_REC___WHSE as WHSE", \
	"FIL_UNCHANGED_REC___REPORT_SHIFT as REPORT_SHIFT", \
	"FIL_UNCHANGED_REC___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_UNCHANGED_REC___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_UNCHANGED_REC___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_UNCHANGED_REC___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_UNCHANGED_REC___VERSION_ID as VERSION_ID", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_REC___ROT_SHIFT_IND)) IN ('Y', '1') THEN '1' ELSE '0' END as ROT_SHIFT_IND_EXP", \
	"FIL_UNCHANGED_REC___ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS", \
	"FIL_UNCHANGED_REC___ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE", \
	"FIL_UNCHANGED_REC___SCHED_SHIFT as SCHED_SHIFT", \
	"FIL_UNCHANGED_REC___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_SHIFT_ID as WM_SHIFT_ID", \
	"FIL_UNCHANGED_REC___WM_SHIFT_CD as WM_SHIFT_CD", \
	"FIL_UNCHANGED_REC___WM_SHIFT_DESC as WM_SHIFT_DESC", \
	"FIL_UNCHANGED_REC___WM_WHSE as WM_WHSE", \
	"FIL_UNCHANGED_REC___WM_SHIFT_EFF_DT as WM_SHIFT_EFF_DT", \
	"FIL_UNCHANGED_REC___OT_FLAG as OT_FLAG", \
	"FIL_UNCHANGED_REC___OT_PAY_FCT1 as OT_PAY_FCT1", \
	"FIL_UNCHANGED_REC___in_WEEK_MIN_OT_HRS as in_WEEK_MIN_OT_HRS", \
	"FIL_UNCHANGED_REC___in_SCHED_SHIFT as in_SCHED_SHIFT", \
	"FIL_UNCHANGED_REC___in_REPORT_SHIFT as in_REPORT_SHIFT", \
	"FIL_UNCHANGED_REC___ROT_SHIFT_FLAG as ROT_SHIFT_FLAG", \
	"FIL_UNCHANGED_REC___in_ROT_SHIFT_NUM_DAYS as in_ROT_SHIFT_NUM_DAYS", \
	"FIL_UNCHANGED_REC___ROT_SHIFT_START_TSTMP as ROT_SHIFT_START_TSTMP", \
	"FIL_UNCHANGED_REC___in_MISC_TXT_1 as in_MISC_TXT_1", \
	"FIL_UNCHANGED_REC___in_MISC_TXT_2 as in_MISC_TXT_2", \
	"FIL_UNCHANGED_REC___in_MISC_NUM_1 as in_MISC_NUM_1", \
	"FIL_UNCHANGED_REC___in_MISC_NUM_2 as in_MISC_NUM_2", \
	"FIL_UNCHANGED_REC___WM_USER_ID as WM_USER_ID", \
	"FIL_UNCHANGED_REC___WM_VERSION_ID as WM_VERSION_ID", \
	"FIL_UNCHANGED_REC___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"FIL_UNCHANGED_REC___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"FIL_UNCHANGED_REC___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_REC___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_REC___DELETE_FLAG as DELETE_FLAG", \
	"FIL_UNCHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_REC___SHIFT_ID IS NULL AND FIL_UNCHANGED_REC___WM_SHIFT_ID IS NOT NULL, 1, 0) as DELETE_FLAG_EXP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_REC___SHIFT_ID IS NOT NULL AND FIL_UNCHANGED_REC___WM_SHIFT_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_REC___SHIFT_ID IS NULL AND FIL_UNCHANGED_REC___WM_SHIFT_ID IS NOT NULL AND ( FIL_UNCHANGED_REC___v_WM_CREATE_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_REC___v_WM_MOD_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_REC___v_WM_CREATED_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_REC___v_WM_LAST_UPDATED_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) ), 'DELETE', IF(FIL_UNCHANGED_REC___SHIFT_ID IS NOT NULL AND FIL_UNCHANGED_REC___WM_SHIFT_ID IS NOT NULL AND ( FIL_UNCHANGED_REC___v_WM_CREATED_TSTMP <> FIL_UNCHANGED_REC___v_CREATED_DTTM OR FIL_UNCHANGED_REC___v_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_REC___v_LAST_UPDATED_DTTM OR FIL_UNCHANGED_REC___v_WM_CREATE_TSTMP <> FIL_UNCHANGED_REC___v_CREATE_DATE_TIME OR FIL_UNCHANGED_REC___v_WM_MOD_TSTMP <> FIL_UNCHANGED_REC___v_MOD_DATE_TIME ), 'UPDATE', NULL))) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node RTR_DELETE, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 54


# Creating output dataframe for RTR_DELETE, output group DELETE
RTR_DELETE_DELETE = EXP_UPD_VALIDATOR.selectExpr( \
	"EXP_UPD_VALIDATOR.LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR.SHIFT_ID as SHIFT_ID", \
	"EXP_UPD_VALIDATOR.EFF_DATE as EFF_DATE", \
	"EXP_UPD_VALIDATOR.SHIFT_CODE as SHIFT_CODE", \
	"EXP_UPD_VALIDATOR.DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR.OT_INDIC_EXP as OT_INDIC_EXP", \
	"EXP_UPD_VALIDATOR.OT_PAY_FCT as OT_PAY_FCT", \
	"EXP_UPD_VALIDATOR.WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS", \
	"EXP_UPD_VALIDATOR.CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPD_VALIDATOR.MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_UPD_VALIDATOR.USER_ID as USER_ID", \
	"EXP_UPD_VALIDATOR.WHSE as WHSE", \
	"EXP_UPD_VALIDATOR.REPORT_SHIFT as REPORT_SHIFT", \
	"EXP_UPD_VALIDATOR.MISC_TXT_1 as MISC_TXT_1", \
	"EXP_UPD_VALIDATOR.MISC_TXT_2 as MISC_TXT_2", \
	"EXP_UPD_VALIDATOR.MISC_NUM_1 as MISC_NUM_1", \
	"EXP_UPD_VALIDATOR.MISC_NUM_2 as MISC_NUM_2", \
	"EXP_UPD_VALIDATOR.VERSION_ID as VERSION_ID", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_IND_EXP as ROT_SHIFT_IND", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE", \
	"EXP_UPD_VALIDATOR.SCHED_SHIFT as SCHED_SHIFT", \
	"EXP_UPD_VALIDATOR.CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR.LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR.in_LOCATION_ID as in_LOCATION_ID", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_ID as WM_SHIFT_ID", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_CD as WM_SHIFT_CD", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_DESC as WM_SHIFT_DESC", \
	"EXP_UPD_VALIDATOR.WM_WHSE as WM_WHSE", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_EFF_DT as WM_SHIFT_EFF_DT", \
	"EXP_UPD_VALIDATOR.OT_FLAG as OT_FLAG", \
	"EXP_UPD_VALIDATOR.OT_PAY_FCT1 as OT_PAY_FCT1", \
	"EXP_UPD_VALIDATOR.in_WEEK_MIN_OT_HRS as in_WEEK_MIN_OT_HRS", \
	"EXP_UPD_VALIDATOR.in_SCHED_SHIFT as in_SCHED_SHIFT", \
	"EXP_UPD_VALIDATOR.in_REPORT_SHIFT as in_REPORT_SHIFT", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_FLAG as ROT_SHIFT_FLAG", \
	"EXP_UPD_VALIDATOR.in_ROT_SHIFT_NUM_DAYS as in_ROT_SHIFT_NUM_DAYS", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_START_TSTMP as ROT_SHIFT_START_TSTMP", \
	"EXP_UPD_VALIDATOR.in_MISC_TXT_1 as in_MISC_TXT_1", \
	"EXP_UPD_VALIDATOR.in_MISC_TXT_2 as in_MISC_TXT_2", \
	"EXP_UPD_VALIDATOR.in_MISC_NUM_1 as in_MISC_NUM_1", \
	"EXP_UPD_VALIDATOR.in_MISC_NUM_2 as in_MISC_NUM_2", \
	"EXP_UPD_VALIDATOR.WM_USER_ID as WM_USER_ID", \
	"EXP_UPD_VALIDATOR.WM_VERSION_ID as WM_VERSION_ID", \
	"EXP_UPD_VALIDATOR.WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"EXP_UPD_VALIDATOR.WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"EXP_UPD_VALIDATOR.WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"EXP_UPD_VALIDATOR.WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"EXP_UPD_VALIDATOR.DELETE_FLAG as DELETE_FLAG", \
	"EXP_UPD_VALIDATOR.in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR.DELETE_FLAG_EXP as DELETE_FLAG_EXP", \
	"EXP_UPD_VALIDATOR.UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR.LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR.o_UPD_VALIDATOR as o_UPD_VALIDATOR").select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID3'), \
	col('SHIFT_ID').alias('SHIFT_ID3'), \
	col('EFF_DATE').alias('EFF_DATE3'), \
	col('SHIFT_CODE').alias('SHIFT_CODE3'), \
	col('DESCRIPTION').alias('DESCRIPTION3'), \
	col('OT_INDIC_EXP').alias('OT_INDIC_EXP3'), \
	col('OT_PAY_FCT').alias('OT_PAY_FCT4'), \
	col('WEEK_MIN_OT_HRS').alias('WEEK_MIN_OT_HRS3'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME3'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME3'), \
	col('USER_ID').alias('USER_ID3'), \
	col('WHSE').alias('WHSE3'), \
	col('REPORT_SHIFT').alias('REPORT_SHIFT3'), \
	col('MISC_TXT_1').alias('MISC_TXT_13'), \
	col('MISC_TXT_2').alias('MISC_TXT_23'), \
	col('MISC_NUM_1').alias('MISC_NUM_13'), \
	col('MISC_NUM_2').alias('MISC_NUM_23'), \
	col('VERSION_ID').alias('VERSION_ID3'), \
	col('ROT_SHIFT_IND').alias('ROT_SHIFT_IND3'), \
	col('ROT_SHIFT_NUM_DAYS').alias('ROT_SHIFT_NUM_DAYS3'), \
	col('ROT_SHIFT_START_DATE').alias('ROT_SHIFT_START_DATE3'), \
	col('SCHED_SHIFT').alias('SCHED_SHIFT3'), \
	col('CREATED_DTTM').alias('CREATED_DTTM3'), \
	col('LAST_UPDATED_DTTM').alias('LAST_UPDATED_DTTM3'), \
	col('in_LOCATION_ID').alias('in_LOCATION_ID3'), \
	col('WM_SHIFT_ID').alias('WM_SHIFT_ID3'), \
	col('WM_SHIFT_CD').alias('WM_SHIFT_CD3'), \
	col('WM_SHIFT_DESC').alias('WM_SHIFT_DESC3'), \
	col('WM_WHSE').alias('WM_WHSE3'), \
	col('WM_SHIFT_EFF_DT').alias('WM_SHIFT_EFF_DT3'), \
	col('OT_FLAG').alias('OT_FLAG3'), \
	col('OT_PAY_FCT1').alias('OT_PAY_FCT13'), \
	col('in_WEEK_MIN_OT_HRS').alias('in_WEEK_MIN_OT_HRS3'), \
	col('in_SCHED_SHIFT').alias('in_SCHED_SHIFT3'), \
	col('in_REPORT_SHIFT').alias('in_REPORT_SHIFT3'), \
	col('ROT_SHIFT_FLAG').alias('ROT_SHIFT_FLAG3'), \
	col('in_ROT_SHIFT_NUM_DAYS').alias('in_ROT_SHIFT_NUM_DAYS3'), \
	col('ROT_SHIFT_START_TSTMP').alias('ROT_SHIFT_START_TSTMP3'), \
	col('in_MISC_TXT_1').alias('in_MISC_TXT_13'), \
	col('in_MISC_TXT_2').alias('in_MISC_TXT_23'), \
	col('in_MISC_NUM_1').alias('in_MISC_NUM_13'), \
	col('in_MISC_NUM_2').alias('in_MISC_NUM_23'), \
	col('WM_USER_ID').alias('WM_USER_ID3'), \
	col('WM_VERSION_ID').alias('WM_VERSION_ID3'), \
	col('WM_CREATE_TSTMP').alias('WM_CREATE_TSTMP3'), \
	col('WM_MOD_TSTMP').alias('WM_MOD_TSTMP3'), \
	col('WM_CREATED_TSTMP').alias('WM_CREATED_TSTMP3'), \
	col('WM_LAST_UPDATED_TSTMP').alias('WM_LAST_UPDATED_TSTMP3'), \
	col('DELETE_FLAG').alias('DELETE_FLAG3'), \
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP3'), \
	col('DELETE_FLAG_EXP').alias('DELETE_FLAG_EXP3'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP3'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP3'), \
	col('o_UPD_VALIDATOR').alias('o_UPD_VALIDATOR3')).filter("o_UPD_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_DELETE, output group INSERT_UPDATE
RTR_DELETE_INSERT_UPDATE = EXP_UPD_VALIDATOR.selectExpr( \
	"EXP_UPD_VALIDATOR.LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR.SHIFT_ID as SHIFT_ID", \
	"EXP_UPD_VALIDATOR.EFF_DATE as EFF_DATE", \
	"EXP_UPD_VALIDATOR.SHIFT_CODE as SHIFT_CODE", \
	"EXP_UPD_VALIDATOR.DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR.OT_INDIC_EXP as OT_INDIC_EXP", \
	"EXP_UPD_VALIDATOR.OT_PAY_FCT as OT_PAY_FCT", \
	"EXP_UPD_VALIDATOR.WEEK_MIN_OT_HRS as WEEK_MIN_OT_HRS", \
	"EXP_UPD_VALIDATOR.CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPD_VALIDATOR.MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_UPD_VALIDATOR.USER_ID as USER_ID", \
	"EXP_UPD_VALIDATOR.WHSE as WHSE", \
	"EXP_UPD_VALIDATOR.REPORT_SHIFT as REPORT_SHIFT", \
	"EXP_UPD_VALIDATOR.MISC_TXT_1 as MISC_TXT_1", \
	"EXP_UPD_VALIDATOR.MISC_TXT_2 as MISC_TXT_2", \
	"EXP_UPD_VALIDATOR.MISC_NUM_1 as MISC_NUM_1", \
	"EXP_UPD_VALIDATOR.MISC_NUM_2 as MISC_NUM_2", \
	"EXP_UPD_VALIDATOR.VERSION_ID as VERSION_ID", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_IND_EXP as ROT_SHIFT_IND", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_NUM_DAYS as ROT_SHIFT_NUM_DAYS", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_START_DATE as ROT_SHIFT_START_DATE", \
	"EXP_UPD_VALIDATOR.SCHED_SHIFT as SCHED_SHIFT", \
	"EXP_UPD_VALIDATOR.CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR.LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR.in_LOCATION_ID as in_LOCATION_ID", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_ID as WM_SHIFT_ID", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_CD as WM_SHIFT_CD", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_DESC as WM_SHIFT_DESC", \
	"EXP_UPD_VALIDATOR.WM_WHSE as WM_WHSE", \
	"EXP_UPD_VALIDATOR.WM_SHIFT_EFF_DT as WM_SHIFT_EFF_DT", \
	"EXP_UPD_VALIDATOR.OT_FLAG as OT_FLAG", \
	"EXP_UPD_VALIDATOR.OT_PAY_FCT1 as OT_PAY_FCT1", \
	"EXP_UPD_VALIDATOR.in_WEEK_MIN_OT_HRS as in_WEEK_MIN_OT_HRS", \
	"EXP_UPD_VALIDATOR.in_SCHED_SHIFT as in_SCHED_SHIFT", \
	"EXP_UPD_VALIDATOR.in_REPORT_SHIFT as in_REPORT_SHIFT", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_FLAG as ROT_SHIFT_FLAG", \
	"EXP_UPD_VALIDATOR.in_ROT_SHIFT_NUM_DAYS as in_ROT_SHIFT_NUM_DAYS", \
	"EXP_UPD_VALIDATOR.ROT_SHIFT_START_TSTMP as ROT_SHIFT_START_TSTMP", \
	"EXP_UPD_VALIDATOR.in_MISC_TXT_1 as in_MISC_TXT_1", \
	"EXP_UPD_VALIDATOR.in_MISC_TXT_2 as in_MISC_TXT_2", \
	"EXP_UPD_VALIDATOR.in_MISC_NUM_1 as in_MISC_NUM_1", \
	"EXP_UPD_VALIDATOR.in_MISC_NUM_2 as in_MISC_NUM_2", \
	"EXP_UPD_VALIDATOR.WM_USER_ID as WM_USER_ID", \
	"EXP_UPD_VALIDATOR.WM_VERSION_ID as WM_VERSION_ID", \
	"EXP_UPD_VALIDATOR.WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"EXP_UPD_VALIDATOR.WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"EXP_UPD_VALIDATOR.WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"EXP_UPD_VALIDATOR.WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"EXP_UPD_VALIDATOR.DELETE_FLAG as DELETE_FLAG", \
	"EXP_UPD_VALIDATOR.in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR.DELETE_FLAG_EXP as DELETE_FLAG_EXP", \
	"EXP_UPD_VALIDATOR.UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR.LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR.o_UPD_VALIDATOR as o_UPD_VALIDATOR").select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID1'), \
	col('SHIFT_ID').alias('SHIFT_ID1'), \
	col('EFF_DATE').alias('EFF_DATE1'), \
	col('SHIFT_CODE').alias('SHIFT_CODE1'), \
	col('DESCRIPTION').alias('DESCRIPTION1'), \
	col('OT_INDIC_EXP').alias('OT_INDIC_EXP1'), \
	col('OT_PAY_FCT').alias('OT_PAY_FCT2'), \
	col('WEEK_MIN_OT_HRS').alias('WEEK_MIN_OT_HRS1'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME1'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME1'), \
	col('USER_ID').alias('USER_ID1'), \
	col('WHSE').alias('WHSE1'), \
	col('REPORT_SHIFT').alias('REPORT_SHIFT1'), \
	col('MISC_TXT_1').alias('MISC_TXT_11'), \
	col('MISC_TXT_2').alias('MISC_TXT_21'), \
	col('MISC_NUM_1').alias('MISC_NUM_11'), \
	col('MISC_NUM_2').alias('MISC_NUM_21'), \
	col('VERSION_ID').alias('VERSION_ID1'), \
	col('ROT_SHIFT_IND').alias('ROT_SHIFT_IND1'), \
	col('ROT_SHIFT_NUM_DAYS').alias('ROT_SHIFT_NUM_DAYS1'), \
	col('ROT_SHIFT_START_DATE').alias('ROT_SHIFT_START_DATE1'), \
	col('SCHED_SHIFT').alias('SCHED_SHIFT1'), \
	col('CREATED_DTTM').alias('CREATED_DTTM1'), \
	col('LAST_UPDATED_DTTM').alias('LAST_UPDATED_DTTM1'), \
	col('in_LOCATION_ID').alias('in_LOCATION_ID1'), \
	col('WM_SHIFT_ID').alias('WM_SHIFT_ID1'), \
	col('WM_SHIFT_CD').alias('WM_SHIFT_CD1'), \
	col('WM_SHIFT_DESC').alias('WM_SHIFT_DESC1'), \
	col('WM_WHSE').alias('WM_WHSE1'), \
	col('WM_SHIFT_EFF_DT').alias('WM_SHIFT_EFF_DT1'), \
	col('OT_FLAG').alias('OT_FLAG1'), \
	col('OT_PAY_FCT1').alias('OT_PAY_FCT11'), \
	col('in_WEEK_MIN_OT_HRS').alias('in_WEEK_MIN_OT_HRS1'), \
	col('in_SCHED_SHIFT').alias('in_SCHED_SHIFT1'), \
	col('in_REPORT_SHIFT').alias('in_REPORT_SHIFT1'), \
	col('ROT_SHIFT_FLAG').alias('ROT_SHIFT_FLAG1'), \
	col('in_ROT_SHIFT_NUM_DAYS').alias('in_ROT_SHIFT_NUM_DAYS1'), \
	col('ROT_SHIFT_START_TSTMP').alias('ROT_SHIFT_START_TSTMP1'), \
	col('in_MISC_TXT_1').alias('in_MISC_TXT_11'), \
	col('in_MISC_TXT_2').alias('in_MISC_TXT_21'), \
	col('in_MISC_NUM_1').alias('in_MISC_NUM_11'), \
	col('in_MISC_NUM_2').alias('in_MISC_NUM_21'), \
	col('WM_USER_ID').alias('WM_USER_ID1'), \
	col('WM_VERSION_ID').alias('WM_VERSION_ID1'), \
	col('WM_CREATE_TSTMP').alias('WM_CREATE_TSTMP1'), \
	col('WM_MOD_TSTMP').alias('WM_MOD_TSTMP1'), \
	col('WM_CREATED_TSTMP').alias('WM_CREATED_TSTMP1'), \
	col('WM_LAST_UPDATED_TSTMP').alias('WM_LAST_UPDATED_TSTMP1'), \
	col('DELETE_FLAG').alias('DELETE_FLAG1'), \
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP1'), \
	col('DELETE_FLAG_EXP').alias('DELETE_FLAG_EXP1'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP1'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP1'), \
	col('o_UPD_VALIDATOR').alias('o_UPD_VALIDATOR1')).filter("o_UPD_VALIDATOR = 'INSERT' OR o_UPD_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_INSERT_UPDATE_temp = RTR_DELETE_INSERT_UPDATE.toDF(*["RTR_DELETE_INSERT_UPDATE___" + col for col in RTR_DELETE_INSERT_UPDATE.columns])

UPD_INS_UPD = RTR_DELETE_INSERT_UPDATE_temp.selectExpr( \
	"RTR_DELETE_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID1", \
	"RTR_DELETE_INSERT_UPDATE___SHIFT_ID1 as SHIFT_ID", \
	"RTR_DELETE_INSERT_UPDATE___SHIFT_CODE1 as SHIFT_CODE", \
	"RTR_DELETE_INSERT_UPDATE___DESCRIPTION1 as DESCRIPTION", \
	"RTR_DELETE_INSERT_UPDATE___WHSE1 as WHSE", \
	"RTR_DELETE_INSERT_UPDATE___EFF_DATE1 as EFF_DATE", \
	"RTR_DELETE_INSERT_UPDATE___OT_INDIC_EXP1 as OT_INDIC_EXP", \
	"RTR_DELETE_INSERT_UPDATE___OT_PAY_FCT2 as OT_PAY_FCT", \
	"RTR_DELETE_INSERT_UPDATE___WEEK_MIN_OT_HRS1 as WEEK_MIN_OT_HRS", \
	"RTR_DELETE_INSERT_UPDATE___SCHED_SHIFT1 as SCHED_SHIFT", \
	"RTR_DELETE_INSERT_UPDATE___REPORT_SHIFT1 as REPORT_SHIFT", \
	"RTR_DELETE_INSERT_UPDATE___ROT_SHIFT_IND1 as ROT_SHIFT_IND1", \
	"RTR_DELETE_INSERT_UPDATE___ROT_SHIFT_NUM_DAYS1 as ROT_SHIFT_NUM_DAYS", \
	"RTR_DELETE_INSERT_UPDATE___ROT_SHIFT_START_DATE1 as ROT_SHIFT_START_DATE", \
	"RTR_DELETE_INSERT_UPDATE___MISC_TXT_11 as MISC_TXT_1", \
	"RTR_DELETE_INSERT_UPDATE___MISC_TXT_21 as MISC_TXT_2", \
	"RTR_DELETE_INSERT_UPDATE___MISC_NUM_11 as MISC_NUM_1", \
	"RTR_DELETE_INSERT_UPDATE___MISC_NUM_21 as MISC_NUM_2", \
	"RTR_DELETE_INSERT_UPDATE___USER_ID1 as USER_ID", \
	"RTR_DELETE_INSERT_UPDATE___VERSION_ID1 as VERSION_ID", \
	"RTR_DELETE_INSERT_UPDATE___CREATE_DATE_TIME1 as CREATE_DATE_TIME", \
	"RTR_DELETE_INSERT_UPDATE___MOD_DATE_TIME1 as MOD_DATE_TIME", \
	"RTR_DELETE_INSERT_UPDATE___CREATED_DTTM1 as CREATED_DTTM", \
	"RTR_DELETE_INSERT_UPDATE___LAST_UPDATED_DTTM1 as LAST_UPDATED_DTTM", \
	"RTR_DELETE_INSERT_UPDATE___DELETE_FLAG_EXP1 as DELETE_FLAG_EXP1", \
	"RTR_DELETE_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP1", \
	"RTR_DELETE_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP1", \
	"RTR_DELETE_INSERT_UPDATE___o_UPD_VALIDATOR1 as o_UPD_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR1') ==(lit('INSERT')), lit(0)).when(col('o_UPD_VALIDATOR1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_SHIFT1, type TARGET 
# COLUMN COUNT: 27

Shortcut_to_WM_E_SHIFT1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(SHIFT_ID AS INT) as WM_SHIFT_ID",
	"CAST(SHIFT_CODE AS STRING) as WM_SHIFT_CD",
	"CAST(DESCRIPTION AS STRING) as WM_SHIFT_DESC",
	"CAST(WHSE AS STRING) as WM_WHSE",
	"CAST(EFF_DATE AS DATE) as WM_SHIFT_EFF_DT",
	"CAST(OT_INDIC_EXP AS TINYINT) as OT_FLAG",
	"CAST(OT_PAY_FCT AS DECIMAL(5,2)) as OT_PAY_FCT",
	"CAST(WEEK_MIN_OT_HRS AS DECIMAL(5,2)) as WEEK_MIN_OT_HRS",
	"CAST(SCHED_SHIFT AS STRING) as SCHED_SHIFT",
	"CAST(REPORT_SHIFT AS STRING) as REPORT_SHIFT",
	"CAST(ROT_SHIFT_IND1 AS TINYINT) as ROT_SHIFT_FLAG",
	"CAST(ROT_SHIFT_NUM_DAYS AS INT) as ROT_SHIFT_NUM_DAYS",
	"CAST(ROT_SHIFT_START_DATE AS TIMESTAMP) as ROT_SHIFT_START_TSTMP",
	"CAST(MISC_TXT_1 AS STRING) as MISC_TXT_1",
	"CAST(MISC_TXT_2 AS STRING) as MISC_TXT_2",
	"CAST(MISC_NUM_1 AS DECIMAL(20,7)) as MISC_NUM_1",
	"CAST(MISC_NUM_2 AS DECIMAL(20,7)) as MISC_NUM_2",
	"CAST(USER_ID AS STRING) as WM_USER_ID",
	"CAST(VERSION_ID AS INT) as WM_VERSION_ID",
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(DELETE_FLAG_EXP1 AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SHIFT_ID = target.WM_SHIFT_ID"""
#   refined_perf_table = "WM_E_SHIFT"
  executeMerge(Shortcut_to_WM_E_SHIFT1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_SHIFT", "WM_E_SHIFT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_SHIFT", "WM_E_SHIFT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_DELETE_temp = RTR_DELETE_DELETE.toDF(*["RTR_DELETE_DELETE___" + col for col in RTR_DELETE_DELETE.columns])

UPD_DELETE = RTR_DELETE_DELETE_temp.selectExpr( \
	"RTR_DELETE_DELETE___in_LOCATION_ID3 as in_LOCATION_ID3", \
	"RTR_DELETE_DELETE___WM_SHIFT_ID3 as WM_SHIFT_ID3", \
	"RTR_DELETE_DELETE___WM_SHIFT_CD3 as WM_SHIFT_CD3", \
	"RTR_DELETE_DELETE___WM_SHIFT_DESC3 as WM_SHIFT_DESC3", \
	"RTR_DELETE_DELETE___WM_WHSE3 as WM_WHSE3", \
	"RTR_DELETE_DELETE___WM_SHIFT_EFF_DT3 as WM_SHIFT_EFF_DT3", \
	"RTR_DELETE_DELETE___OT_FLAG3 as OT_FLAG3", \
	"RTR_DELETE_DELETE___OT_PAY_FCT13 as OT_PAY_FCT13", \
	"RTR_DELETE_DELETE___in_WEEK_MIN_OT_HRS3 as in_WEEK_MIN_OT_HRS3", \
	"RTR_DELETE_DELETE___in_SCHED_SHIFT3 as in_SCHED_SHIFT3", \
	"RTR_DELETE_DELETE___in_REPORT_SHIFT3 as in_REPORT_SHIFT3", \
	"RTR_DELETE_DELETE___ROT_SHIFT_FLAG3 as ROT_SHIFT_FLAG3", \
	"RTR_DELETE_DELETE___in_ROT_SHIFT_NUM_DAYS3 as in_ROT_SHIFT_NUM_DAYS3", \
	"RTR_DELETE_DELETE___ROT_SHIFT_START_TSTMP3 as ROT_SHIFT_START_TSTMP3", \
	"RTR_DELETE_DELETE___in_MISC_TXT_13 as in_MISC_TXT_13", \
	"RTR_DELETE_DELETE___in_MISC_TXT_23 as in_MISC_TXT_23", \
	"RTR_DELETE_DELETE___in_MISC_NUM_13 as in_MISC_NUM_13", \
	"RTR_DELETE_DELETE___in_MISC_NUM_23 as in_MISC_NUM_23", \
	"RTR_DELETE_DELETE___WM_USER_ID3 as WM_USER_ID3", \
	"RTR_DELETE_DELETE___WM_VERSION_ID3 as WM_VERSION_ID3", \
	"RTR_DELETE_DELETE___WM_CREATE_TSTMP3 as WM_CREATE_TSTMP3", \
	"RTR_DELETE_DELETE___WM_MOD_TSTMP3 as WM_MOD_TSTMP3", \
	"RTR_DELETE_DELETE___WM_CREATED_TSTMP3 as WM_CREATED_TSTMP3", \
	"RTR_DELETE_DELETE___WM_LAST_UPDATED_TSTMP3 as WM_LAST_UPDATED_TSTMP3", \
	"RTR_DELETE_DELETE___DELETE_FLAG_EXP3 as DELETE_FLAG_EXP3", \
	"RTR_DELETE_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3", \
	"RTR_DELETE_DELETE___LOAD_TSTMP3 as LOAD_TSTMP3", \
	"RTR_DELETE_DELETE___o_UPD_VALIDATOR3 as o_UPD_VALIDATOR3") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_SHIFT2, type TARGET 
# COLUMN COUNT: 27


# Shortcut_to_WM_E_SHIFT2 = UPD_DELETE.selectExpr( \
# 	"CAST(in_LOCATION_ID3 AS BIGINT) as LOCATION_ID", \
# 	"CAST(WM_SHIFT_ID3 AS BIGINT) as WM_SHIFT_ID", \
# 	"CAST(null AS STRING) as WM_SHIFT_CD", \
# 	"CAST(null AS STRING) as WM_SHIFT_DESC", \
# 	"CAST(null AS STRING) as WM_WHSE", \
# 	"CAST(null AS DATE) as WM_SHIFT_EFF_DT", \
# 	"CAST(null AS BIGINT) as OT_FLAG", \
# 	"CAST(null AS BIGINT) as OT_PAY_FCT", \
# 	"CAST(null AS BIGINT) as WEEK_MIN_OT_HRS", \
# 	"CAST(null AS STRING) as SCHED_SHIFT", \
# 	"CAST(null AS STRING) as REPORT_SHIFT", \
# 	"CAST(null AS BIGINT) as ROT_SHIFT_FLAG", \
# 	"CAST(null AS BIGINT) as ROT_SHIFT_NUM_DAYS", \
# 	"CAST(null AS TIMESTAMP) as ROT_SHIFT_START_TSTMP", \
# 	"CAST(null AS STRING) as MISC_TXT_1", \
# 	"CAST(null AS STRING) as MISC_TXT_2", \
# 	"CAST(null AS BIGINT) as MISC_NUM_1", \
# 	"CAST(null AS BIGINT) as MISC_NUM_2", \
# 	"CAST(null AS STRING) as WM_USER_ID", \
# 	"CAST(null AS BIGINT) as WM_VERSION_ID", \
# 	"CAST(null AS TIMESTAMP) as WM_CREATE_TSTMP", \
# 	"CAST(null AS TIMESTAMP) as WM_MOD_TSTMP", \
# 	"CAST(null AS TIMESTAMP) as WM_CREATED_TSTMP", \
# 	"CAST(null AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", \
# 	"CAST(DELETE_FLAG_EXP3 AS BIGINT) as DELETE_FLAG", \
# 	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(null AS TIMESTAMP) as LOAD_TSTMP" \
# )
# Shortcut_to_WM_E_SHIFT2.write.saveAsTable(f'{raw}.WM_E_SHIFT', mode = 'append')