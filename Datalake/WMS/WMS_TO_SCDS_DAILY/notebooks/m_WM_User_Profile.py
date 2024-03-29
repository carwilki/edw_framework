#Code converted on 2023-06-22 21:03:28
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
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_USER_PROFILE"
raw_perf_table = f"{raw}.WM_USER_PROFILE_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_USER_PROFILE, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_WM_USER_PROFILE = spark.sql(f"""SELECT
LOCATION_ID,
WM_USER_PROFILE_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_USER_PROFILE_ID IN (SELECT USER_PROFILE_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_USER_PROFILE_PRE, type SOURCE 
# COLUMN COUNT: 51

SQ_Shortcut_to_WM_USER_PROFILE_PRE = spark.sql(f"""SELECT
DC_NBR,
USER_PROFILE_ID,
LOGIN_USER_ID,
MENU_ID,
EMPLYE_ID,
RESTR_TASK_GRP_TO_DFLT,
RESTR_MENU_MODE_TO_DFLT,
DFLT_RF_MENU_MODE,
LANG_ID,
DATE_MASK,
LAST_TASK,
LAST_LOCN,
LAST_WORK_GRP,
LAST_WORK_AREA,
ALLOW_TASK_INT_CHG,
NBR_OF_TASK_TO_DSP,
TASK_DSP_MODE,
DB_USER_ID,
DB_PSWD,
DB_CONNECT_STRING,
PRTR_REQSTR,
IDLE_TIME_BEF_SHTDWN,
RF_MENU_ID,
USER_ID,
CREATE_DATE_TIME,
MOD_DATE_TIME,
PAGE_SIZE,
VOCOLLECT_WORK_TYPE,
CURR_TASK_GRP,
CURR_VOCOLLECT_PTS_CASE,
CURR_VOCOLLECT_REASON_CODE,
TASK_GRP_JUMP_FLAG,
AUTO_3PL_LOGIN_FLAG,
SECURITY_CONTEXT_ID,
SEC_USER_NAME,
VOCOLLECT_PUTAWAY_FLAG,
VOCOLLECT_REPLEN_FLAG,
VOCOLLECT_PACKING_FLAG,
CLS_TIMEZONE_ID,
DAL_CONNECTION_STRING,
WM_VERSION_ID,
USER_SECURITY_CONTEXT_ID,
DFLT_TASK_INT,
CREATED_DTTM,
LAST_UPDATED_DTTM,
MOBILE_HELP_TEXT,
MOB_SPLASH_SCREEN_FLAG,
SCREEN_TYPE_ID,
MOBILE_MSG_SPEECH_LEVEL,
MOBILE_MSG_SPEECH_RATE,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 51

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_USER_PROFILE_PRE_temp = SQ_Shortcut_to_WM_USER_PROFILE_PRE.toDF(*["SQ_Shortcut_to_WM_USER_PROFILE_PRE___" + col for col in SQ_Shortcut_to_WM_USER_PROFILE_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_USER_PROFILE_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_USER_PROFILE_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___USER_PROFILE_ID as USER_PROFILE_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LOGIN_USER_ID as LOGIN_USER_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___MENU_ID as MENU_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___EMPLYE_ID as EMPLYE_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___RESTR_TASK_GRP_TO_DFLT as RESTR_TASK_GRP_TO_DFLT", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___RESTR_MENU_MODE_TO_DFLT as RESTR_MENU_MODE_TO_DFLT", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DFLT_RF_MENU_MODE as DFLT_RF_MENU_MODE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LANG_ID as LANG_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DATE_MASK as DATE_MASK", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LAST_TASK as LAST_TASK", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LAST_LOCN as LAST_LOCN", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LAST_WORK_GRP as LAST_WORK_GRP", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LAST_WORK_AREA as LAST_WORK_AREA", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___ALLOW_TASK_INT_CHG as ALLOW_TASK_INT_CHG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___NBR_OF_TASK_TO_DSP as NBR_OF_TASK_TO_DSP", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___TASK_DSP_MODE as TASK_DSP_MODE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DB_USER_ID as DB_USER_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DB_PSWD as DB_PSWD", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DB_CONNECT_STRING as DB_CONNECT_STRING", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___PRTR_REQSTR as PRTR_REQSTR", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___IDLE_TIME_BEF_SHTDWN as IDLE_TIME_BEF_SHTDWN", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___RF_MENU_ID as RF_MENU_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___USER_ID as USER_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___PAGE_SIZE as PAGE_SIZE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___VOCOLLECT_WORK_TYPE as VOCOLLECT_WORK_TYPE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___CURR_TASK_GRP as CURR_TASK_GRP", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___CURR_VOCOLLECT_PTS_CASE as CURR_VOCOLLECT_PTS_CASE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___CURR_VOCOLLECT_REASON_CODE as CURR_VOCOLLECT_REASON_CODE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___TASK_GRP_JUMP_FLAG as TASK_GRP_JUMP_FLAG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___AUTO_3PL_LOGIN_FLAG as AUTO_3PL_LOGIN_FLAG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___SECURITY_CONTEXT_ID as SECURITY_CONTEXT_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___SEC_USER_NAME as SEC_USER_NAME", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___VOCOLLECT_PUTAWAY_FLAG as VOCOLLECT_PUTAWAY_FLAG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___VOCOLLECT_REPLEN_FLAG as VOCOLLECT_REPLEN_FLAG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___VOCOLLECT_PACKING_FLAG as VOCOLLECT_PACKING_FLAG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___CLS_TIMEZONE_ID as CLS_TIMEZONE_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DAL_CONNECTION_STRING as DAL_CONNECTION_STRING", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___WM_VERSION_ID as WM_VERSION_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___USER_SECURITY_CONTEXT_ID as USER_SECURITY_CONTEXT_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___DFLT_TASK_INT as DFLT_TASK_INT", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___MOBILE_HELP_TEXT as MOBILE_HELP_TEXT", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___MOB_SPLASH_SCREEN_FLAG as MOB_SPLASH_SCREEN_FLAG", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___SCREEN_TYPE_ID as SCREEN_TYPE_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___MOBILE_MSG_SPEECH_LEVEL as MOBILE_MSG_SPEECH_LEVEL", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___MOBILE_MSG_SPEECH_RATE as MOBILE_MSG_SPEECH_RATE", 
	"SQ_Shortcut_to_WM_USER_PROFILE_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 52

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_USER_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 57

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_USER_PROFILE_temp = SQ_Shortcut_to_WM_USER_PROFILE.toDF(*["SQ_Shortcut_to_WM_USER_PROFILE___" + col for col in SQ_Shortcut_to_WM_USER_PROFILE.columns])

JNR_WM_USER_PROFILE = SQ_Shortcut_to_WM_USER_PROFILE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_USER_PROFILE_temp.SQ_Shortcut_to_WM_USER_PROFILE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_USER_PROFILE_temp.SQ_Shortcut_to_WM_USER_PROFILE___WM_USER_PROFILE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___USER_PROFILE_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___USER_PROFILE_ID as USER_PROFILE_ID", 
	"JNR_SITE_PROFILE___LOGIN_USER_ID as LOGIN_USER_ID", 
	"JNR_SITE_PROFILE___MENU_ID as MENU_ID", 
	"JNR_SITE_PROFILE___EMPLYE_ID as EMPLYE_ID", 
	"JNR_SITE_PROFILE___RESTR_TASK_GRP_TO_DFLT as RESTR_TASK_GRP_TO_DFLT", 
	"JNR_SITE_PROFILE___RESTR_MENU_MODE_TO_DFLT as RESTR_MENU_MODE_TO_DFLT", 
	"JNR_SITE_PROFILE___DFLT_RF_MENU_MODE as DFLT_RF_MENU_MODE", 
	"JNR_SITE_PROFILE___LANG_ID as LANG_ID", 
	"JNR_SITE_PROFILE___DATE_MASK as DATE_MASK", 
	"JNR_SITE_PROFILE___LAST_TASK as LAST_TASK", 
	"JNR_SITE_PROFILE___LAST_LOCN as LAST_LOCN", 
	"JNR_SITE_PROFILE___LAST_WORK_GRP as LAST_WORK_GRP", 
	"JNR_SITE_PROFILE___LAST_WORK_AREA as LAST_WORK_AREA", 
	"JNR_SITE_PROFILE___ALLOW_TASK_INT_CHG as ALLOW_TASK_INT_CHG", 
	"JNR_SITE_PROFILE___NBR_OF_TASK_TO_DSP as NBR_OF_TASK_TO_DSP", 
	"JNR_SITE_PROFILE___TASK_DSP_MODE as TASK_DSP_MODE", 
	"JNR_SITE_PROFILE___DB_USER_ID as DB_USER_ID", 
	"JNR_SITE_PROFILE___DB_PSWD as DB_PSWD", 
	"JNR_SITE_PROFILE___DB_CONNECT_STRING as DB_CONNECT_STRING", 
	"JNR_SITE_PROFILE___PRTR_REQSTR as PRTR_REQSTR", 
	"JNR_SITE_PROFILE___IDLE_TIME_BEF_SHTDWN as IDLE_TIME_BEF_SHTDWN", 
	"JNR_SITE_PROFILE___RF_MENU_ID as RF_MENU_ID", 
	"JNR_SITE_PROFILE___USER_ID as USER_ID", 
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_SITE_PROFILE___PAGE_SIZE as PAGE_SIZE", 
	"JNR_SITE_PROFILE___VOCOLLECT_WORK_TYPE as VOCOLLECT_WORK_TYPE", 
	"JNR_SITE_PROFILE___CURR_TASK_GRP as CURR_TASK_GRP", 
	"JNR_SITE_PROFILE___CURR_VOCOLLECT_PTS_CASE as CURR_VOCOLLECT_PTS_CASE", 
	"JNR_SITE_PROFILE___CURR_VOCOLLECT_REASON_CODE as CURR_VOCOLLECT_REASON_CODE", 
	"JNR_SITE_PROFILE___TASK_GRP_JUMP_FLAG as TASK_GRP_JUMP_FLAG", 
	"JNR_SITE_PROFILE___AUTO_3PL_LOGIN_FLAG as AUTO_3PL_LOGIN_FLAG", 
	"JNR_SITE_PROFILE___SECURITY_CONTEXT_ID as SECURITY_CONTEXT_ID", 
	"JNR_SITE_PROFILE___SEC_USER_NAME as SEC_USER_NAME", 
	"JNR_SITE_PROFILE___VOCOLLECT_PUTAWAY_FLAG as VOCOLLECT_PUTAWAY_FLAG", 
	"JNR_SITE_PROFILE___VOCOLLECT_REPLEN_FLAG as VOCOLLECT_REPLEN_FLAG", 
	"JNR_SITE_PROFILE___VOCOLLECT_PACKING_FLAG as VOCOLLECT_PACKING_FLAG", 
	"JNR_SITE_PROFILE___CLS_TIMEZONE_ID as CLS_TIMEZONE_ID", 
	"JNR_SITE_PROFILE___DAL_CONNECTION_STRING as DAL_CONNECTION_STRING", 
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", 
	"JNR_SITE_PROFILE___USER_SECURITY_CONTEXT_ID as USER_SECURITY_CONTEXT_ID", 
	"JNR_SITE_PROFILE___DFLT_TASK_INT as DFLT_TASK_INT", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___MOBILE_HELP_TEXT as MOBILE_HELP_TEXT", 
	"JNR_SITE_PROFILE___MOB_SPLASH_SCREEN_FLAG as MOB_SPLASH_SCREEN_FLAG", 
	"JNR_SITE_PROFILE___SCREEN_TYPE_ID as SCREEN_TYPE_ID", 
	"JNR_SITE_PROFILE___MOBILE_MSG_SPEECH_LEVEL as MOBILE_MSG_SPEECH_LEVEL", 
	"JNR_SITE_PROFILE___MOBILE_MSG_SPEECH_RATE as MOBILE_MSG_SPEECH_RATE", 
	"SQ_Shortcut_to_WM_USER_PROFILE___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE___WM_USER_PROFILE_ID as i_WM_USER_PROFILE_ID", 
	"SQ_Shortcut_to_WM_USER_PROFILE___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_USER_PROFILE___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_USER_PROFILE___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"SQ_Shortcut_to_WM_USER_PROFILE___WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"SQ_Shortcut_to_WM_USER_PROFILE___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 57

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_USER_PROFILE_temp = JNR_WM_USER_PROFILE.toDF(*["JNR_WM_USER_PROFILE___" + col for col in JNR_WM_USER_PROFILE.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_USER_PROFILE_temp.selectExpr( 
	"JNR_WM_USER_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_USER_PROFILE___USER_PROFILE_ID as USER_PROFILE_ID", 
	"JNR_WM_USER_PROFILE___LOGIN_USER_ID as LOGIN_USER_ID", 
	"JNR_WM_USER_PROFILE___MENU_ID as MENU_ID", 
	"JNR_WM_USER_PROFILE___EMPLYE_ID as EMPLYE_ID", 
	"JNR_WM_USER_PROFILE___RESTR_TASK_GRP_TO_DFLT as RESTR_TASK_GRP_TO_DFLT", 
	"JNR_WM_USER_PROFILE___RESTR_MENU_MODE_TO_DFLT as RESTR_MENU_MODE_TO_DFLT", 
	"JNR_WM_USER_PROFILE___DFLT_RF_MENU_MODE as DFLT_RF_MENU_MODE", 
	"JNR_WM_USER_PROFILE___LANG_ID as LANG_ID", 
	"JNR_WM_USER_PROFILE___DATE_MASK as DATE_MASK", 
	"JNR_WM_USER_PROFILE___LAST_TASK as LAST_TASK", 
	"JNR_WM_USER_PROFILE___LAST_LOCN as LAST_LOCN", 
	"JNR_WM_USER_PROFILE___LAST_WORK_GRP as LAST_WORK_GRP", 
	"JNR_WM_USER_PROFILE___LAST_WORK_AREA as LAST_WORK_AREA", 
	"JNR_WM_USER_PROFILE___ALLOW_TASK_INT_CHG as ALLOW_TASK_INT_CHG", 
	"JNR_WM_USER_PROFILE___NBR_OF_TASK_TO_DSP as NBR_OF_TASK_TO_DSP", 
	"JNR_WM_USER_PROFILE___TASK_DSP_MODE as TASK_DSP_MODE", 
	"JNR_WM_USER_PROFILE___DB_USER_ID as DB_USER_ID", 
	"JNR_WM_USER_PROFILE___DB_PSWD as DB_PSWD", 
	"JNR_WM_USER_PROFILE___DB_CONNECT_STRING as DB_CONNECT_STRING", 
	"JNR_WM_USER_PROFILE___PRTR_REQSTR as PRTR_REQSTR", 
	"JNR_WM_USER_PROFILE___IDLE_TIME_BEF_SHTDWN as IDLE_TIME_BEF_SHTDWN", 
	"JNR_WM_USER_PROFILE___RF_MENU_ID as RF_MENU_ID", 
	"JNR_WM_USER_PROFILE___USER_ID as USER_ID", 
	"JNR_WM_USER_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_WM_USER_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_WM_USER_PROFILE___PAGE_SIZE as PAGE_SIZE", 
	"JNR_WM_USER_PROFILE___VOCOLLECT_WORK_TYPE as VOCOLLECT_WORK_TYPE", 
	"JNR_WM_USER_PROFILE___CURR_TASK_GRP as CURR_TASK_GRP", 
	"JNR_WM_USER_PROFILE___CURR_VOCOLLECT_PTS_CASE as CURR_VOCOLLECT_PTS_CASE", 
	"JNR_WM_USER_PROFILE___CURR_VOCOLLECT_REASON_CODE as CURR_VOCOLLECT_REASON_CODE", 
	"JNR_WM_USER_PROFILE___TASK_GRP_JUMP_FLAG as TASK_GRP_JUMP_FLAG", 
	"JNR_WM_USER_PROFILE___AUTO_3PL_LOGIN_FLAG as AUTO_3PL_LOGIN_FLAG", 
	"JNR_WM_USER_PROFILE___SECURITY_CONTEXT_ID as SECURITY_CONTEXT_ID", 
	"JNR_WM_USER_PROFILE___SEC_USER_NAME as SEC_USER_NAME", 
	"JNR_WM_USER_PROFILE___VOCOLLECT_PUTAWAY_FLAG as VOCOLLECT_PUTAWAY_FLAG", 
	"JNR_WM_USER_PROFILE___VOCOLLECT_REPLEN_FLAG as VOCOLLECT_REPLEN_FLAG", 
	"JNR_WM_USER_PROFILE___VOCOLLECT_PACKING_FLAG as VOCOLLECT_PACKING_FLAG", 
	"JNR_WM_USER_PROFILE___CLS_TIMEZONE_ID as CLS_TIMEZONE_ID", 
	"JNR_WM_USER_PROFILE___DAL_CONNECTION_STRING as DAL_CONNECTION_STRING", 
	"JNR_WM_USER_PROFILE___WM_VERSION_ID as WM_VERSION_ID", 
	"JNR_WM_USER_PROFILE___USER_SECURITY_CONTEXT_ID as USER_SECURITY_CONTEXT_ID", 
	"JNR_WM_USER_PROFILE___DFLT_TASK_INT as DFLT_TASK_INT", 
	"JNR_WM_USER_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_USER_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_USER_PROFILE___MOBILE_HELP_TEXT as MOBILE_HELP_TEXT", 
	"JNR_WM_USER_PROFILE___MOB_SPLASH_SCREEN_FLAG as MOB_SPLASH_SCREEN_FLAG", 
	"JNR_WM_USER_PROFILE___SCREEN_TYPE_ID as SCREEN_TYPE_ID", 
	"JNR_WM_USER_PROFILE___MOBILE_MSG_SPEECH_LEVEL as MOBILE_MSG_SPEECH_LEVEL", 
	"JNR_WM_USER_PROFILE___MOBILE_MSG_SPEECH_RATE as MOBILE_MSG_SPEECH_RATE", 
	"JNR_WM_USER_PROFILE___i_LOCATION_ID as i_LOCATION_ID", 
	"JNR_WM_USER_PROFILE___i_WM_USER_PROFILE_ID as i_WM_USER_PROFILE_ID", 
	"JNR_WM_USER_PROFILE___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_USER_PROFILE___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_USER_PROFILE___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"JNR_WM_USER_PROFILE___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"JNR_WM_USER_PROFILE___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_USER_PROFILE_ID IS NULL OR (NOT i_WM_USER_PROFILE_ID IS NULL AND (COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01')) OR (COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')) OR (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 56

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___USER_PROFILE_ID as USER_PROFILE_ID", 
	"FIL_UNCHANGED_RECORDS___LOGIN_USER_ID as LOGIN_USER_ID", 
	"FIL_UNCHANGED_RECORDS___MENU_ID as MENU_ID", 
	"FIL_UNCHANGED_RECORDS___EMPLYE_ID as EMPLYE_ID", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___RESTR_TASK_GRP_TO_DFLT)) IN ('Y', '1') THEN '1' ELSE '0' END as RESTR_TASK_GRP_TO_DFLT_EXP", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___RESTR_MENU_MODE_TO_DFLT)) IN ('Y', '1') THEN '1' ELSE '0' END as RESTR_MENU_MODE_TO_DFLT_EXP", 
	"FIL_UNCHANGED_RECORDS___DFLT_RF_MENU_MODE as DFLT_RF_MENU_MODE", 
	"FIL_UNCHANGED_RECORDS___LANG_ID as LANG_ID", 
	"FIL_UNCHANGED_RECORDS___DATE_MASK as DATE_MASK", 
	"FIL_UNCHANGED_RECORDS___LAST_TASK as LAST_TASK", 
	"FIL_UNCHANGED_RECORDS___LAST_LOCN as LAST_LOCN", 
	"FIL_UNCHANGED_RECORDS___LAST_WORK_GRP as LAST_WORK_GRP", 
	"FIL_UNCHANGED_RECORDS___LAST_WORK_AREA as LAST_WORK_AREA", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___ALLOW_TASK_INT_CHG)) IN ('Y', '1') THEN '1' ELSE '0' END as ALLOW_TASK_INT_CHG_EXP", 
	"FIL_UNCHANGED_RECORDS___NBR_OF_TASK_TO_DSP as NBR_OF_TASK_TO_DSP", 
	"FIL_UNCHANGED_RECORDS___TASK_DSP_MODE as TASK_DSP_MODE", 
	"FIL_UNCHANGED_RECORDS___DB_USER_ID as DB_USER_ID", 
	"FIL_UNCHANGED_RECORDS___DB_PSWD as DB_PSWD", 
	"FIL_UNCHANGED_RECORDS___DB_CONNECT_STRING as DB_CONNECT_STRING", 
	"FIL_UNCHANGED_RECORDS___PRTR_REQSTR as PRTR_REQSTR", 
	"FIL_UNCHANGED_RECORDS___IDLE_TIME_BEF_SHTDWN as IDLE_TIME_BEF_SHTDWN", 
	"FIL_UNCHANGED_RECORDS___RF_MENU_ID as RF_MENU_ID", 
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", 
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___PAGE_SIZE as PAGE_SIZE", 
	"FIL_UNCHANGED_RECORDS___VOCOLLECT_WORK_TYPE as VOCOLLECT_WORK_TYPE", 
	"FIL_UNCHANGED_RECORDS___CURR_TASK_GRP as CURR_TASK_GRP", 
	"FIL_UNCHANGED_RECORDS___CURR_VOCOLLECT_PTS_CASE as CURR_VOCOLLECT_PTS_CASE", 
	"FIL_UNCHANGED_RECORDS___CURR_VOCOLLECT_REASON_CODE as CURR_VOCOLLECT_REASON_CODE", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___TASK_GRP_JUMP_FLAG)) IN ('Y', '1') THEN '1' ELSE '0' END as TASK_GRP_JUMP_FLAG_EXP", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___AUTO_3PL_LOGIN_FLAG)) IN ('Y', '1') THEN '1' ELSE '0' END as AUTO_3PL_LOGIN_FLAG_EXP", 
	"FIL_UNCHANGED_RECORDS___SECURITY_CONTEXT_ID as SECURITY_CONTEXT_ID", 
	"FIL_UNCHANGED_RECORDS___SEC_USER_NAME as SEC_USER_NAME", 
	"FIL_UNCHANGED_RECORDS___VOCOLLECT_PUTAWAY_FLAG as VOCOLLECT_PUTAWAY_FLAG", 
	"FIL_UNCHANGED_RECORDS___VOCOLLECT_REPLEN_FLAG as VOCOLLECT_REPLEN_FLAG", 
	"FIL_UNCHANGED_RECORDS___VOCOLLECT_PACKING_FLAG as VOCOLLECT_PACKING_FLAG", 
	"FIL_UNCHANGED_RECORDS___CLS_TIMEZONE_ID as CLS_TIMEZONE_ID", 
	"FIL_UNCHANGED_RECORDS___DAL_CONNECTION_STRING as DAL_CONNECTION_STRING", 
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID as WM_VERSION_ID", 
	"FIL_UNCHANGED_RECORDS___USER_SECURITY_CONTEXT_ID as USER_SECURITY_CONTEXT_ID", 
	"FIL_UNCHANGED_RECORDS___DFLT_TASK_INT as DFLT_TASK_INT", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___MOBILE_HELP_TEXT)) IN ('Y', '1') THEN '1' ELSE '0' END as MOBILE_HELP_TEXT_EXP", 
	"FIL_UNCHANGED_RECORDS___MOB_SPLASH_SCREEN_FLAG as MOB_SPLASH_SCREEN_FLAG", 
	"FIL_UNCHANGED_RECORDS___SCREEN_TYPE_ID as SCREEN_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___MOBILE_MSG_SPEECH_LEVEL as MOBILE_MSG_SPEECH_LEVEL", 
	"FIL_UNCHANGED_RECORDS___MOBILE_MSG_SPEECH_RATE as MOBILE_MSG_SPEECH_RATE", 
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID as i_LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_USER_PROFILE_ID as i_WM_USER_PROFILE_ID", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_USER_PROFILE_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 53

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPD_VALIDATOR___USER_PROFILE_ID as USER_PROFILE_ID", 
	"EXP_UPD_VALIDATOR___LOGIN_USER_ID as LOGIN_USER_ID", 
	"EXP_UPD_VALIDATOR___MENU_ID as MENU_ID", 
	"EXP_UPD_VALIDATOR___EMPLYE_ID as EMPLYE_ID", 
	"EXP_UPD_VALIDATOR___RESTR_TASK_GRP_TO_DFLT_EXP as RESTR_TASK_GRP_TO_DFLT", 
	"EXP_UPD_VALIDATOR___RESTR_MENU_MODE_TO_DFLT_EXP as RESTR_MENU_MODE_TO_DFLT", 
	"EXP_UPD_VALIDATOR___DFLT_RF_MENU_MODE as DFLT_RF_MENU_MODE", 
	"EXP_UPD_VALIDATOR___LANG_ID as LANG_ID", 
	"EXP_UPD_VALIDATOR___DATE_MASK as DATE_MASK", 
	"EXP_UPD_VALIDATOR___LAST_TASK as LAST_TASK", 
	"EXP_UPD_VALIDATOR___LAST_LOCN as LAST_LOCN", 
	"EXP_UPD_VALIDATOR___LAST_WORK_GRP as LAST_WORK_GRP", 
	"EXP_UPD_VALIDATOR___LAST_WORK_AREA as LAST_WORK_AREA", 
	"EXP_UPD_VALIDATOR___ALLOW_TASK_INT_CHG_EXP as ALLOW_TASK_INT_CHG", 
	"EXP_UPD_VALIDATOR___NBR_OF_TASK_TO_DSP as NBR_OF_TASK_TO_DSP", 
	"EXP_UPD_VALIDATOR___TASK_DSP_MODE as TASK_DSP_MODE", 
	"EXP_UPD_VALIDATOR___DB_USER_ID as DB_USER_ID", 
	"EXP_UPD_VALIDATOR___DB_PSWD as DB_PSWD", 
	"EXP_UPD_VALIDATOR___DB_CONNECT_STRING as DB_CONNECT_STRING", 
	"EXP_UPD_VALIDATOR___PRTR_REQSTR as PRTR_REQSTR", 
	"EXP_UPD_VALIDATOR___IDLE_TIME_BEF_SHTDWN as IDLE_TIME_BEF_SHTDWN", 
	"EXP_UPD_VALIDATOR___RF_MENU_ID as RF_MENU_ID", 
	"EXP_UPD_VALIDATOR___USER_ID as USER_ID", 
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"EXP_UPD_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", 
	"EXP_UPD_VALIDATOR___PAGE_SIZE as PAGE_SIZE", 
	"EXP_UPD_VALIDATOR___VOCOLLECT_WORK_TYPE as VOCOLLECT_WORK_TYPE", 
	"EXP_UPD_VALIDATOR___CURR_TASK_GRP as CURR_TASK_GRP", 
	"EXP_UPD_VALIDATOR___CURR_VOCOLLECT_PTS_CASE as CURR_VOCOLLECT_PTS_CASE", 
	"EXP_UPD_VALIDATOR___CURR_VOCOLLECT_REASON_CODE as CURR_VOCOLLECT_REASON_CODE", 
	"EXP_UPD_VALIDATOR___TASK_GRP_JUMP_FLAG_EXP as TASK_GRP_JUMP_FLAG", 
	"EXP_UPD_VALIDATOR___AUTO_3PL_LOGIN_FLAG_EXP as AUTO_3PL_LOGIN_FLAG", 
	"EXP_UPD_VALIDATOR___SECURITY_CONTEXT_ID as SECURITY_CONTEXT_ID", 
	"EXP_UPD_VALIDATOR___SEC_USER_NAME as SEC_USER_NAME", 
	"EXP_UPD_VALIDATOR___VOCOLLECT_PUTAWAY_FLAG as VOCOLLECT_PUTAWAY_FLAG", 
	"EXP_UPD_VALIDATOR___VOCOLLECT_REPLEN_FLAG as VOCOLLECT_REPLEN_FLAG", 
	"EXP_UPD_VALIDATOR___VOCOLLECT_PACKING_FLAG as VOCOLLECT_PACKING_FLAG", 
	"EXP_UPD_VALIDATOR___CLS_TIMEZONE_ID as CLS_TIMEZONE_ID", 
	"EXP_UPD_VALIDATOR___DAL_CONNECTION_STRING as DAL_CONNECTION_STRING", 
	"EXP_UPD_VALIDATOR___WM_VERSION_ID as WM_VERSION_ID", 
	"EXP_UPD_VALIDATOR___USER_SECURITY_CONTEXT_ID as USER_SECURITY_CONTEXT_ID", 
	"EXP_UPD_VALIDATOR___DFLT_TASK_INT as DFLT_TASK_INT", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___MOBILE_HELP_TEXT_EXP as MOBILE_HELP_TEXT", 
	"EXP_UPD_VALIDATOR___MOB_SPLASH_SCREEN_FLAG as MOB_SPLASH_SCREEN_FLAG", 
	"EXP_UPD_VALIDATOR___SCREEN_TYPE_ID as SCREEN_TYPE_ID", 
	"EXP_UPD_VALIDATOR___MOBILE_MSG_SPEECH_LEVEL as MOBILE_MSG_SPEECH_LEVEL", 
	"EXP_UPD_VALIDATOR___MOBILE_MSG_SPEECH_RATE as MOBILE_MSG_SPEECH_RATE", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_USER_PROFILE1, type TARGET 
# COLUMN COUNT: 52


Shortcut_to_WM_USER_PROFILE1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(USER_PROFILE_ID AS INT) as WM_USER_PROFILE_ID",
	"CAST(LOGIN_USER_ID AS STRING) as WM_LOGIN_USER_ID",
	"CAST(MENU_ID AS INT) as WM_MENU_ID",
	"CAST(EMPLYE_ID AS STRING) as WM_EMPLOYEE_ID",
	"CAST(RESTR_TASK_GRP_TO_DFLT AS TINYINT) as RESTR_TASK_GRP_TO_DFLT_FLAG",
	"CAST(RESTR_MENU_MODE_TO_DFLT AS TINYINT) as RESTR_MENU_MODE_TO_DFLT_FLAG",
	"CAST(DFLT_RF_MENU_MODE AS STRING) as WM_DFLT_RF_MENU_MODE",
	"CAST(LANG_ID AS STRING) as WM_LANG_ID",
	"CAST(DATE_MASK AS STRING) as WM_DATE_MASK",
	"CAST(LAST_TASK AS STRING) as WM_LAST_TASK_ID",
	"CAST(LAST_LOCN AS STRING) as WM_LAST_LOCN_ID",
	"CAST(LAST_WORK_GRP AS STRING) as WM_LAST_WORK_GRP",
	"CAST(LAST_WORK_AREA AS STRING) as WM_LAST_WORK_AREA",
	"CAST(ALLOW_TASK_INT_CHG AS TINYINT) as ALLOW_TASK_INT_CHG_FLAG",
	"CAST(NBR_OF_TASK_TO_DSP AS TINYINT) as NBR_OF_TASK_TO_DSP",
	"CAST(TASK_DSP_MODE AS STRING) as WM_TASK_DSP_MODE",
	"CAST(DB_USER_ID AS STRING) as DB_USER_ID",
	"CAST(DB_PSWD AS STRING) as DB_PSWD",
	"CAST(DB_CONNECT_STRING AS STRING) as DB_CONNECT_STRING",
	"CAST(PRTR_REQSTR AS STRING) as PRTR_REQSTR",
	"CAST(IDLE_TIME_BEF_SHTDWN AS INT) as IDLE_TIME_BEF_SHTDWN",
	"CAST(RF_MENU_ID AS INT) as WM_RF_MENU_ID",
	"CAST(PAGE_SIZE AS INT) as PAGE_SIZE",
	"CAST(VOCOLLECT_WORK_TYPE AS TINYINT) as WM_VOCOLLECT_WORK_TYPE",
	"CAST(CURR_TASK_GRP AS STRING) as WM_CURR_TASK_GRP",
	"CAST(CURR_VOCOLLECT_PTS_CASE AS STRING) as WM_CURR_VOCOLLECT_PTS_CASE",
	"CAST(CURR_VOCOLLECT_REASON_CODE AS STRING) as WM_CURR_VOCOLLECT_REASON_CD",
	"CAST(TASK_GRP_JUMP_FLAG AS TINYINT) as TASK_GRP_JUMP_FLAG",
	"CAST(AUTO_3PL_LOGIN_FLAG AS TINYINT) as AUTO_3PL_LOGIN_FLAG",
	"CAST(SECURITY_CONTEXT_ID AS INT) as WM_SECURITY_CONTEXT_ID",
	"CAST(SEC_USER_NAME AS STRING) as SEC_USER_NAME",
	"CAST(VOCOLLECT_PUTAWAY_FLAG AS TINYINT) as VOCOLLECT_PUTAWAY_FLAG",
	"CAST(VOCOLLECT_REPLEN_FLAG AS TINYINT) as VOCOLLECT_REPLEN_FLAG",
	"CAST(VOCOLLECT_PACKING_FLAG AS TINYINT) as VOCOLLECT_PACKING_FLAG",
	"CAST(CLS_TIMEZONE_ID AS INT) as WM_CLS_TIMEZONE_ID",
	"CAST(DAL_CONNECTION_STRING AS STRING) as DAL_CONNECTION_STRING",
	"CAST(USER_SECURITY_CONTEXT_ID AS INT) as WM_USER_SECURITY_CONTEXT_ID",
	"CAST(DFLT_TASK_INT AS SMALLINT) as DFLT_TASK_INT",
	"CAST(MOBILE_HELP_TEXT AS TINYINT) as MOBILE_HELP_TEXT_FLAG",
	"CAST(MOB_SPLASH_SCREEN_FLAG AS TINYINT) as MOB_SPLASH_SCREEN_FLAG",
	"CAST(SCREEN_TYPE_ID AS SMALLINT) as WM_SCREEN_TYPE_ID",
	"CAST(MOBILE_MSG_SPEECH_LEVEL AS STRING) as MOBILE_MSG_SPEECH_LEVEL",
	"CAST(MOBILE_MSG_SPEECH_RATE AS STRING) as MOBILE_MSG_SPEECH_RATE",
	"CAST(USER_ID AS STRING) as WM_USER_ID",
	"CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_USER_PROFILE_ID = target.WM_USER_PROFILE_ID"""
  # refined_perf_table = "WM_USER_PROFILE"
  executeMerge(Shortcut_to_WM_USER_PROFILE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_USER_PROFILE", "WM_USER_PROFILE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_USER_PROFILE", "WM_USER_PROFILE","Failed",str(e), f"{raw}.log_run_details", )
  raise e

	