#Code converted on 2023-06-26 09:58:17
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
# Processing node SQ_Shortcut_to_WM_E_EMP_DTL_PRE, type SOURCE 
# COLUMN COUNT: 32

SQ_Shortcut_to_WM_E_EMP_DTL_PRE = spark.sql(f"""SELECT
WM_E_EMP_DTL_PRE.DC_NBR,
WM_E_EMP_DTL_PRE.EMP_DTL_ID,
WM_E_EMP_DTL_PRE.EMP_ID,
WM_E_EMP_DTL_PRE.EFF_DATE_TIME,
WM_E_EMP_DTL_PRE.EMP_STAT_ID,
WM_E_EMP_DTL_PRE.PAY_RATE,
WM_E_EMP_DTL_PRE.PAY_SCALE_ID,
WM_E_EMP_DTL_PRE.SPVSR_EMP_ID,
WM_E_EMP_DTL_PRE.DEPT_ID,
WM_E_EMP_DTL_PRE.SHIFT_ID,
WM_E_EMP_DTL_PRE.ROLE_ID,
WM_E_EMP_DTL_PRE.USER_DEF_FIELD_1,
WM_E_EMP_DTL_PRE.USER_DEF_FIELD_2,
WM_E_EMP_DTL_PRE.CMNT,
WM_E_EMP_DTL_PRE.CREATE_DATE_TIME,
WM_E_EMP_DTL_PRE.MOD_DATE_TIME,
WM_E_EMP_DTL_PRE.USER_ID,
WM_E_EMP_DTL_PRE.WHSE,
WM_E_EMP_DTL_PRE.JOB_FUNC_ID,
WM_E_EMP_DTL_PRE.STARTUP_TIME,
WM_E_EMP_DTL_PRE.CLEANUP_TIME,
WM_E_EMP_DTL_PRE.MISC_TXT_1,
WM_E_EMP_DTL_PRE.MISC_TXT_2,
WM_E_EMP_DTL_PRE.MISC_NUM_1,
WM_E_EMP_DTL_PRE.MISC_NUM_2,
WM_E_EMP_DTL_PRE.DFLT_PERF_GOAL,
WM_E_EMP_DTL_PRE.VERSION_ID,
WM_E_EMP_DTL_PRE.IS_SUPER,
WM_E_EMP_DTL_PRE.CREATED_DTTM,
WM_E_EMP_DTL_PRE.LAST_UPDATED_DTTM,
WM_E_EMP_DTL_PRE.EXCLUDE_AUTO_CICO,
WM_E_EMP_DTL_PRE.LOAD_TSTMP
FROM WM_E_EMP_DTL_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_EMP_DTL, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_WM_E_EMP_DTL = spark.sql(f"""SELECT
WM_E_EMP_DTL.LOCATION_ID,
WM_E_EMP_DTL.WM_EMP_DTL_ID,
WM_E_EMP_DTL.WM_CREATED_TSTMP,
WM_E_EMP_DTL.WM_LAST_UPDATED_TSTMP,
WM_E_EMP_DTL.WM_CREATE_TSTMP,
WM_E_EMP_DTL.WM_MOD_TSTMP,
WM_E_EMP_DTL.LOAD_TSTMP
FROM WM_E_EMP_DTL
WHERE WM_EMP_DTL_ID IN (SELECT EMP_DTL_ID FROM WM_E_EMP_DTL_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_EMP_DTL_PRE_temp = SQ_Shortcut_to_WM_E_EMP_DTL_PRE.toDF(*["SQ_Shortcut_to_WM_E_EMP_DTL_PRE___" + col for col in SQ_Shortcut_to_WM_E_EMP_DTL_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_EMP_DTL_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___DC_NBR as in_DC_NBR", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EMP_DTL_ID as EMP_DTL_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EMP_ID as EMP_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EFF_DATE_TIME as EFF_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EMP_STAT_ID as EMP_STAT_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___PAY_RATE as PAY_RATE", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___PAY_SCALE_ID as PAY_SCALE_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___SPVSR_EMP_ID as SPVSR_EMP_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___DEPT_ID as DEPT_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___SHIFT_ID as SHIFT_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___ROLE_ID as ROLE_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CMNT as CMNT", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___JOB_FUNC_ID as JOB_FUNC_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___STARTUP_TIME as STARTUP_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CLEANUP_TIME as CLEANUP_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___DFLT_PERF_GOAL as DFLT_PERF_GOAL", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___IS_SUPER as IS_SUPER", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EXCLUDE_AUTO_CICO as EXCLUDE_AUTO_CICO", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___LOAD_TSTMP as LOAD_TSTMP").selectExpr( \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_EMP_DTL_PRE___in_DC_NBR as int) as DC_NBR", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EMP_DTL_ID as EMP_DTL_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EMP_ID as EMP_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EFF_DATE_TIME as EFF_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EMP_STAT_ID as EMP_STAT_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___PAY_RATE as PAY_RATE", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___PAY_SCALE_ID as PAY_SCALE_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___SPVSR_EMP_ID as SPVSR_EMP_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___DEPT_ID as DEPT_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___SHIFT_ID as SHIFT_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___ROLE_ID as ROLE_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CMNT as CMNT", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___JOB_FUNC_ID as JOB_FUNC_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___STARTUP_TIME as STARTUP_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CLEANUP_TIME as CLEANUP_TIME", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___DFLT_PERF_GOAL as DFLT_PERF_GOAL", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___IS_SUPER as IS_SUPER", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___EXCLUDE_AUTO_CICO as EXCLUDE_AUTO_CICO", \
	"SQ_Shortcut_to_WM_E_EMP_DTL_PRE___LOAD_TSTMP as LOAD_TSTMP" \
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
# COLUMN COUNT: 34

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_EMP_DTL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 38

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_E_EMP_DTL_temp = SQ_Shortcut_to_WM_E_EMP_DTL.toDF(*["SQ_Shortcut_to_WM_E_EMP_DTL___" + col for col in SQ_Shortcut_to_WM_E_EMP_DTL.columns])

JNR_WM_E_EMP_DTL = SQ_Shortcut_to_WM_E_EMP_DTL_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_EMP_DTL_temp.SQ_Shortcut_to_WM_E_EMP_DTL___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_EMP_DTL_temp.SQ_Shortcut_to_WM_E_EMP_DTL___WM_EMP_DTL_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___EMP_DTL_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___EMP_DTL_ID as EMP_DTL_ID", \
	"JNR_SITE_PROFILE___EMP_ID as EMP_ID", \
	"JNR_SITE_PROFILE___EFF_DATE_TIME as EFF_DATE_TIME", \
	"JNR_SITE_PROFILE___EMP_STAT_ID as EMP_STAT_ID", \
	"JNR_SITE_PROFILE___PAY_RATE as PAY_RATE", \
	"JNR_SITE_PROFILE___PAY_SCALE_ID as PAY_SCALE_ID", \
	"JNR_SITE_PROFILE___SPVSR_EMP_ID as SPVSR_EMP_ID", \
	"JNR_SITE_PROFILE___DEPT_ID as DEPT_ID", \
	"JNR_SITE_PROFILE___SHIFT_ID as SHIFT_ID", \
	"JNR_SITE_PROFILE___ROLE_ID as ROLE_ID", \
	"JNR_SITE_PROFILE___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", \
	"JNR_SITE_PROFILE___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", \
	"JNR_SITE_PROFILE___CMNT as CMNT", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___JOB_FUNC_ID as JOB_FUNC_ID", \
	"JNR_SITE_PROFILE___STARTUP_TIME as STARTUP_TIME", \
	"JNR_SITE_PROFILE___CLEANUP_TIME as CLEANUP_TIME", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___DFLT_PERF_GOAL as DFLT_PERF_GOAL", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___IS_SUPER as IS_SUPER", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___EXCLUDE_AUTO_CICO as EXCLUDE_AUTO_CICO", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___WM_EMP_DTL_ID as in_WM_EMP_DTL_ID", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___LOAD_TSTMP as in_LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_E_EMP_DTL___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 37

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_EMP_DTL_temp = JNR_WM_E_EMP_DTL.toDF(*["JNR_WM_E_EMP_DTL___" + col for col in JNR_WM_E_EMP_DTL.columns])

FIL_NO_CHANGE_REC = JNR_WM_E_EMP_DTL_temp.selectExpr( \
	"JNR_WM_E_EMP_DTL___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_EMP_DTL___EMP_DTL_ID as EMP_DTL_ID", \
	"JNR_WM_E_EMP_DTL___EMP_ID as EMP_ID", \
	"JNR_WM_E_EMP_DTL___EFF_DATE_TIME as EFF_DATE_TIME", \
	"JNR_WM_E_EMP_DTL___EMP_STAT_ID as EMP_STAT_ID", \
	"JNR_WM_E_EMP_DTL___PAY_RATE as PAY_RATE", \
	"JNR_WM_E_EMP_DTL___PAY_SCALE_ID as PAY_SCALE_ID", \
	"JNR_WM_E_EMP_DTL___SPVSR_EMP_ID as SPVSR_EMP_ID", \
	"JNR_WM_E_EMP_DTL___DEPT_ID as DEPT_ID", \
	"JNR_WM_E_EMP_DTL___SHIFT_ID as SHIFT_ID", \
	"JNR_WM_E_EMP_DTL___ROLE_ID as ROLE_ID", \
	"JNR_WM_E_EMP_DTL___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", \
	"JNR_WM_E_EMP_DTL___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", \
	"JNR_WM_E_EMP_DTL___CMNT as CMNT", \
	"JNR_WM_E_EMP_DTL___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_EMP_DTL___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_EMP_DTL___USER_ID as USER_ID", \
	"JNR_WM_E_EMP_DTL___WHSE as WHSE", \
	"JNR_WM_E_EMP_DTL___JOB_FUNC_ID as JOB_FUNC_ID", \
	"JNR_WM_E_EMP_DTL___STARTUP_TIME as STARTUP_TIME", \
	"JNR_WM_E_EMP_DTL___CLEANUP_TIME as CLEANUP_TIME", \
	"JNR_WM_E_EMP_DTL___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_EMP_DTL___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_EMP_DTL___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_EMP_DTL___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_EMP_DTL___DFLT_PERF_GOAL as DFLT_PERF_GOAL", \
	"JNR_WM_E_EMP_DTL___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_EMP_DTL___IS_SUPER as IS_SUPER", \
	"JNR_WM_E_EMP_DTL___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_E_EMP_DTL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_E_EMP_DTL___EXCLUDE_AUTO_CICO as EXCLUDE_AUTO_CICO", \
	"JNR_WM_E_EMP_DTL___in_WM_EMP_DTL_ID as in_WM_EMP_DTL_ID", \
	"JNR_WM_E_EMP_DTL___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"JNR_WM_E_EMP_DTL___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"JNR_WM_E_EMP_DTL___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"JNR_WM_E_EMP_DTL___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_E_EMP_DTL___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP")\
    .filter("in_WM_EMP_DTL_ID is Null OR (  in_WM_EMP_DTL_ID is not Null() AND \
             ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(WM_MOD_TSTMP, date'1900-01-01')\
             OR COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01')\
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___EMP_DTL_ID as EMP_DTL_ID", \
	"FIL_NO_CHANGE_REC___EMP_ID as EMP_ID", \
	"FIL_NO_CHANGE_REC___EFF_DATE_TIME as EFF_DATE_TIME", \
	"FIL_NO_CHANGE_REC___EMP_STAT_ID as EMP_STAT_ID", \
	"FIL_NO_CHANGE_REC___PAY_RATE as PAY_RATE", \
	"FIL_NO_CHANGE_REC___PAY_SCALE_ID as PAY_SCALE_ID", \
	"FIL_NO_CHANGE_REC___SPVSR_EMP_ID as SPVSR_EMP_ID", \
	"FIL_NO_CHANGE_REC___DEPT_ID as DEPT_ID", \
	"FIL_NO_CHANGE_REC___SHIFT_ID as SHIFT_ID", \
	"FIL_NO_CHANGE_REC___ROLE_ID as ROLE_ID", \
	"FIL_NO_CHANGE_REC___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", \
	"FIL_NO_CHANGE_REC___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", \
	"FIL_NO_CHANGE_REC___CMNT as CMNT", \
	"FIL_NO_CHANGE_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___USER_ID as USER_ID", \
	"FIL_NO_CHANGE_REC___WHSE as WHSE", \
	"FIL_NO_CHANGE_REC___JOB_FUNC_ID as JOB_FUNC_ID", \
	"FIL_NO_CHANGE_REC___STARTUP_TIME as STARTUP_TIME", \
	"FIL_NO_CHANGE_REC___CLEANUP_TIME as CLEANUP_TIME", \
	"FIL_NO_CHANGE_REC___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___DFLT_PERF_GOAL as DFLT_PERF_GOAL", \
	"FIL_NO_CHANGE_REC___VERSION_ID as VERSION_ID", \
	"FIL_NO_CHANGE_REC___IS_SUPER as IS_SUPER", \
	"DECODE ( LTRIM ( RTRIM ( UPPER ( FIL_NO_CHANGE_REC___IS_SUPER ) ) ) , 'Y','1' , '1','1','0' ) as IS_SUPER_O", \
	"FIL_NO_CHANGE_REC___CREATED_DTTM as CREATED_DTTM", \
	"FIL_NO_CHANGE_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_NO_CHANGE_REC___EXCLUDE_AUTO_CICO as EXCLUDE_AUTO_CICO", \
	"DECODE ( LTRIM ( RTRIM ( UPPER ( FIL_NO_CHANGE_REC___EXCLUDE_AUTO_CICO ) ) ) , 'Y','1' , '1','1','0' ) as EXCLUDE_AUTO_CICO_O", \
	"IF (FIL_NO_CHANGE_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_EMP_DTL_ID as in_WM_EMP_DTL_ID" \
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr( \
	"EXP_EVAL_VALUES___LOCATION_ID as LOCATION_ID", \
	"EXP_EVAL_VALUES___EMP_DTL_ID as EMP_DTL_ID", \
	"EXP_EVAL_VALUES___EMP_ID as EMP_ID", \
	"EXP_EVAL_VALUES___EFF_DATE_TIME as EFF_DATE_TIME", \
	"EXP_EVAL_VALUES___EMP_STAT_ID as EMP_STAT_ID", \
	"EXP_EVAL_VALUES___PAY_RATE as PAY_RATE", \
	"EXP_EVAL_VALUES___PAY_SCALE_ID as PAY_SCALE_ID", \
	"EXP_EVAL_VALUES___SPVSR_EMP_ID as SPVSR_EMP_ID", \
	"EXP_EVAL_VALUES___DEPT_ID as DEPT_ID", \
	"EXP_EVAL_VALUES___SHIFT_ID as SHIFT_ID", \
	"EXP_EVAL_VALUES___ROLE_ID as ROLE_ID", \
	"EXP_EVAL_VALUES___USER_DEF_FIELD_1 as USER_DEF_FIELD_1", \
	"EXP_EVAL_VALUES___USER_DEF_FIELD_2 as USER_DEF_FIELD_2", \
	"EXP_EVAL_VALUES___CMNT as CMNT", \
	"EXP_EVAL_VALUES___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_EVAL_VALUES___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_EVAL_VALUES___USER_ID as USER_ID", \
	"EXP_EVAL_VALUES___WHSE as WHSE", \
	"EXP_EVAL_VALUES___JOB_FUNC_ID as JOB_FUNC_ID", \
	"EXP_EVAL_VALUES___STARTUP_TIME as STARTUP_TIME", \
	"EXP_EVAL_VALUES___CLEANUP_TIME as CLEANUP_TIME", \
	"EXP_EVAL_VALUES___MISC_TXT_1 as MISC_TXT_1", \
	"EXP_EVAL_VALUES___MISC_TXT_2 as MISC_TXT_2", \
	"EXP_EVAL_VALUES___MISC_NUM_1 as MISC_NUM_1", \
	"EXP_EVAL_VALUES___MISC_NUM_2 as MISC_NUM_2", \
	"EXP_EVAL_VALUES___DFLT_PERF_GOAL as DFLT_PERF_GOAL", \
	"EXP_EVAL_VALUES___VERSION_ID as VERSION_ID", \
	"EXP_EVAL_VALUES___IS_SUPER_O as IS_SUPER", \
	"EXP_EVAL_VALUES___CREATED_DTTM as CREATED_DTTM", \
	"EXP_EVAL_VALUES___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_EVAL_VALUES___EXCLUDE_AUTO_CICO_O as EXCLUDE_AUTO_CICO", \
	"EXP_EVAL_VALUES___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVAL_VALUES___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_EVAL_VALUES___in_WM_EMP_DTL_ID as in_WM_EMP_DTL_ID") \
	.withColumn('pyspark_data_action', when((in_WM_EMP_DTL_ID.isNull()) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_EMP_DTL, type TARGET 
# COLUMN COUNT: 33

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_EMP_DTL_ID = target.WM_EMP_DTL_ID"""
  refined_perf_table = "WM_E_EMP_DTL"
  executeMerge(UPD_VALIDATE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_EMP_DTL", "WM_E_EMP_DTL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_EMP_DTL", "WM_E_EMP_DTL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	