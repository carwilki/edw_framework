#Code converted on 2023-06-26 09:56:05
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
# Processing node SQ_Shortcut_to_WM_E_MSRMNT, type SOURCE 
# COLUMN COUNT: 22

SQ_Shortcut_to_WM_E_MSRMNT = spark.sql(f"""SELECT
WM_E_MSRMNT.LOCATION_ID,
WM_E_MSRMNT.WM_MSRMNT_ID,
WM_E_MSRMNT.WM_MSRMNT_CD,
WM_E_MSRMNT.WM_MSRMNT_NAME,
WM_E_MSRMNT.WM_ORIG_MSRMNT_CD,
WM_E_MSRMNT.WM_ORIG_MSRMNT_NAME,
WM_E_MSRMNT.WM_MSRMNT_STATUS_CD,
WM_E_MSRMNT.SYS_CREATED_FLAG,
WM_E_MSRMNT.WM_UNIQUE_SEED_ID,
WM_E_MSRMNT.SIMULATION_DC_NAME,
WM_E_MSRMNT.MISC_TXT_1,
WM_E_MSRMNT.MISC_TXT_2,
WM_E_MSRMNT.MISC_NUM_1,
WM_E_MSRMNT.MISC_NUM_2,
WM_E_MSRMNT.WM_USER_ID,
WM_E_MSRMNT.WM_VERSION_ID,
WM_E_MSRMNT.WM_CREATED_TSTMP,
WM_E_MSRMNT.WM_LAST_UPDATED_TSTMP,
WM_E_MSRMNT.WM_CREATE_TSTMP,
WM_E_MSRMNT.WM_MOD_TSTMP,
WM_E_MSRMNT.UPDATE_TSTMP,
WM_E_MSRMNT.LOAD_TSTMP
FROM WM_E_MSRMNT
WHERE WM_MSRMNT_ID IN (SELECT MSRMNT_ID FROM WM_E_MSRMNT_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_MSRMNT_PRE, type SOURCE 
# COLUMN COUNT: 21

SQ_Shortcut_to_WM_E_MSRMNT_PRE = spark.sql(f"""SELECT
WM_E_MSRMNT_PRE.DC_NBR,
WM_E_MSRMNT_PRE.MSRMNT_ID,
WM_E_MSRMNT_PRE.MSRMNT_CODE,
WM_E_MSRMNT_PRE.NAME,
WM_E_MSRMNT_PRE.STATUS_FLAG,
WM_E_MSRMNT_PRE.SYS_CREATED,
WM_E_MSRMNT_PRE.CREATE_DATE_TIME,
WM_E_MSRMNT_PRE.MOD_DATE_TIME,
WM_E_MSRMNT_PRE.USER_ID,
WM_E_MSRMNT_PRE.MISC_TXT_1,
WM_E_MSRMNT_PRE.MISC_TXT_2,
WM_E_MSRMNT_PRE.MISC_NUM_1,
WM_E_MSRMNT_PRE.MISC_NUM_2,
WM_E_MSRMNT_PRE.VERSION_ID,
WM_E_MSRMNT_PRE.UNQ_SEED_ID,
WM_E_MSRMNT_PRE.SIM_WHSE,
WM_E_MSRMNT_PRE.ORIG_MSRMNT_CODE,
WM_E_MSRMNT_PRE.ORIG_NAME,
WM_E_MSRMNT_PRE.CREATED_DTTM,
WM_E_MSRMNT_PRE.LAST_UPDATED_DTTM,
WM_E_MSRMNT_PRE.LOAD_TSTMP
FROM WM_E_MSRMNT_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_MSRMNT_PRE_temp = SQ_Shortcut_to_WM_E_MSRMNT_PRE.toDF(*["SQ_Shortcut_to_WM_E_MSRMNT_PRE___" + col for col in SQ_Shortcut_to_WM_E_MSRMNT_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_MSRMNT_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_MSRMNT_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MSRMNT_ID as MSRMNT_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MSRMNT_CODE as MSRMNT_CODE", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___NAME as NAME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___STATUS_FLAG as STATUS_FLAG", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___SYS_CREATED as SYS_CREATED", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___UNQ_SEED_ID as UNQ_SEED_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___SIM_WHSE as SIM_WHSE", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___ORIG_MSRMNT_CODE as ORIG_MSRMNT_CODE", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___ORIG_NAME as ORIG_NAME", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_MSRMNT_PRE___LOAD_TSTMP as LOAD_TSTMP" \
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
# COLUMN COUNT: 23

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_MSRMNT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 42

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_MSRMNT_temp = SQ_Shortcut_to_WM_E_MSRMNT.toDF(*["SQ_Shortcut_to_WM_E_MSRMNT___" + col for col in SQ_Shortcut_to_WM_E_MSRMNT.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_E_MSRMNT = SQ_Shortcut_to_WM_E_MSRMNT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_MSRMNT_temp.SQ_Shortcut_to_WM_E_MSRMNT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_MSRMNT_temp.SQ_Shortcut_to_WM_E_MSRMNT___WM_MSRMNT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___MSRMNT_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___MSRMNT_ID as MSRMNT_ID", \
	"JNR_SITE_PROFILE___MSRMNT_CODE as MSRMNT_CODE", \
	"JNR_SITE_PROFILE___NAME as NAME", \
	"JNR_SITE_PROFILE___STATUS_FLAG as STATUS_FLAG", \
	"JNR_SITE_PROFILE___SYS_CREATED as SYS_CREATED", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___UNQ_SEED_ID as UNQ_SEED_ID", \
	"JNR_SITE_PROFILE___SIM_WHSE as SIM_WHSE", \
	"JNR_SITE_PROFILE___ORIG_MSRMNT_CODE as ORIG_MSRMNT_CODE", \
	"JNR_SITE_PROFILE___ORIG_NAME as ORIG_NAME", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_MSRMNT___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_MSRMNT_ID as in_MSRMNT_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_MSRMNT_CD as in_MSRMNT_CODE", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_MSRMNT_NAME as in_NAME", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_MSRMNT_STATUS_CD as in_STATUS_FLAG", \
	"SQ_Shortcut_to_WM_E_MSRMNT___SYS_CREATED_FLAG as in_SYS_CREATED", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_CREATE_TSTMP as in_CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_MOD_TSTMP as in_MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_USER_ID as in_USER_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT___MISC_TXT_1 as in_MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT___MISC_TXT_2 as in_MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT___MISC_NUM_1 as in_MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_MSRMNT___MISC_NUM_2 as in_MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_VERSION_ID as in_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_UNIQUE_SEED_ID as in_UNQ_SEED_ID", \
	"SQ_Shortcut_to_WM_E_MSRMNT___SIMULATION_DC_NAME as in_SIM_WHSE", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_ORIG_MSRMNT_CD as in_ORIG_MSRMNT_CODE", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_ORIG_MSRMNT_NAME as in_ORIG_NAME", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_CREATED_TSTMP as in_CREATED_DTTM", \
	"SQ_Shortcut_to_WM_E_MSRMNT___WM_LAST_UPDATED_TSTMP as in_LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_MSRMNT___UPDATE_TSTMP as in_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_MSRMNT___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 42

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_MSRMNT_temp = JNR_WM_E_MSRMNT.toDF(*["JNR_WM_E_MSRMNT___" + col for col in JNR_WM_E_MSRMNT.columns])

FIL_NO_CHANGE_REC = JNR_WM_E_MSRMNT_temp.selectExpr( \
	"JNR_WM_E_MSRMNT___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_MSRMNT___MSRMNT_ID as MSRMNT_ID", \
	"JNR_WM_E_MSRMNT___MSRMNT_CODE as MSRMNT_CODE", \
	"JNR_WM_E_MSRMNT___NAME as NAME", \
	"JNR_WM_E_MSRMNT___STATUS_FLAG as STATUS_FLAG", \
	"JNR_WM_E_MSRMNT___SYS_CREATED as SYS_CREATED", \
	"JNR_WM_E_MSRMNT___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_MSRMNT___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_MSRMNT___USER_ID as USER_ID", \
	"JNR_WM_E_MSRMNT___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_MSRMNT___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_MSRMNT___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_MSRMNT___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_MSRMNT___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_MSRMNT___UNQ_SEED_ID as UNQ_SEED_ID", \
	"JNR_WM_E_MSRMNT___SIM_WHSE as SIM_WHSE", \
	"JNR_WM_E_MSRMNT___ORIG_MSRMNT_CODE as ORIG_MSRMNT_CODE", \
	"JNR_WM_E_MSRMNT___ORIG_NAME as ORIG_NAME", \
	"JNR_WM_E_MSRMNT___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_E_MSRMNT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_E_MSRMNT___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_E_MSRMNT___in_MSRMNT_ID as in_MSRMNT_ID", \
	"JNR_WM_E_MSRMNT___in_MSRMNT_CODE as in_MSRMNT_CODE", \
	"JNR_WM_E_MSRMNT___in_NAME as in_NAME", \
	"JNR_WM_E_MSRMNT___in_STATUS_FLAG as in_STATUS_FLAG", \
	"JNR_WM_E_MSRMNT___in_SYS_CREATED as in_SYS_CREATED", \
	"JNR_WM_E_MSRMNT___in_CREATE_DATE_TIME as in_CREATE_DATE_TIME", \
	"JNR_WM_E_MSRMNT___in_MOD_DATE_TIME as in_MOD_DATE_TIME", \
	"JNR_WM_E_MSRMNT___in_USER_ID as in_USER_ID", \
	"JNR_WM_E_MSRMNT___in_MISC_TXT_1 as in_MISC_TXT_1", \
	"JNR_WM_E_MSRMNT___in_MISC_TXT_2 as in_MISC_TXT_2", \
	"JNR_WM_E_MSRMNT___in_MISC_NUM_1 as in_MISC_NUM_1", \
	"JNR_WM_E_MSRMNT___in_MISC_NUM_2 as in_MISC_NUM_2", \
	"JNR_WM_E_MSRMNT___in_VERSION_ID as in_VERSION_ID", \
	"JNR_WM_E_MSRMNT___in_UNQ_SEED_ID as in_UNQ_SEED_ID", \
	"JNR_WM_E_MSRMNT___in_SIM_WHSE as in_SIM_WHSE", \
	"JNR_WM_E_MSRMNT___in_ORIG_MSRMNT_CODE as in_ORIG_MSRMNT_CODE", \
	"JNR_WM_E_MSRMNT___in_ORIG_NAME as in_ORIG_NAME", \
	"JNR_WM_E_MSRMNT___in_CREATED_DTTM as in_CREATED_DTTM", \
	"JNR_WM_E_MSRMNT___in_LAST_UPDATED_DTTM as in_LAST_UPDATED_DTTM", \
	"JNR_WM_E_MSRMNT___in_UPDATE_TSTMP as in_UPDATE_TSTMP", \
	"JNR_WM_E_MSRMNT___in_LOAD_TSTMP as in_LOAD_TSTMP")\
    .filter("in_MSRMNT_ID IS NULL OR ( (  in_MSRMNT_ID IS NOT NULL ) AND \
            ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(in_CREATE_DATE_TIME, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(in_MOD_DATE_TIME, date'1900-01-01')\
             OR COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(in_CREATED_DTTM, date'1900-01-01')\
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(in_LAST_UPDATED_DTTM, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 43

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___MSRMNT_ID as MSRMNT_ID", \
	"FIL_NO_CHANGE_REC___MSRMNT_CODE as MSRMNT_CODE", \
	"FIL_NO_CHANGE_REC___NAME as NAME", \
	"FIL_NO_CHANGE_REC___STATUS_FLAG as STATUS_FLAG", \
	"DECODE ( LTRIM ( RTRIM ( UPPER ( FIL_NO_CHANGE_REC___SYS_CREATED ) ) ) , 'Y','1' , '1','1','0' ) as SYS_CREATED_FLAG", \
	"FIL_NO_CHANGE_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___USER_ID as USER_ID", \
	"FIL_NO_CHANGE_REC___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___VERSION_ID as VERSION_ID", \
	"FIL_NO_CHANGE_REC___UNQ_SEED_ID as UNQ_SEED_ID", \
	"FIL_NO_CHANGE_REC___SIM_WHSE as SIM_WHSE", \
	"FIL_NO_CHANGE_REC___ORIG_MSRMNT_CODE as ORIG_MSRMNT_CODE", \
	"FIL_NO_CHANGE_REC___ORIG_NAME as ORIG_NAME", \
	"FIL_NO_CHANGE_REC___CREATED_DTTM as CREATED_DTTM", \
	"FIL_NO_CHANGE_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_NO_CHANGE_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_NO_CHANGE_REC___in_MSRMNT_ID as in_MSRMNT_ID", \
	"FIL_NO_CHANGE_REC___in_MSRMNT_CODE as in_MSRMNT_CODE", \
	"FIL_NO_CHANGE_REC___in_NAME as in_NAME", \
	"FIL_NO_CHANGE_REC___in_STATUS_FLAG as in_STATUS_FLAG", \
	"FIL_NO_CHANGE_REC___in_SYS_CREATED as in_SYS_CREATED", \
	"FIL_NO_CHANGE_REC___in_CREATE_DATE_TIME as in_CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___in_MOD_DATE_TIME as in_MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___in_USER_ID as in_USER_ID", \
	"FIL_NO_CHANGE_REC___in_MISC_TXT_1 as in_MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___in_MISC_TXT_2 as in_MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___in_MISC_NUM_1 as in_MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___in_MISC_NUM_2 as in_MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___in_VERSION_ID as in_VERSION_ID", \
	"FIL_NO_CHANGE_REC___in_UNQ_SEED_ID as in_UNQ_SEED_ID", \
	"FIL_NO_CHANGE_REC___in_SIM_WHSE as in_SIM_WHSE", \
	"FIL_NO_CHANGE_REC___in_ORIG_MSRMNT_CODE as in_ORIG_MSRMNT_CODE", \
	"FIL_NO_CHANGE_REC___in_ORIG_NAME as in_ORIG_NAME", \
	"FIL_NO_CHANGE_REC___in_CREATED_DTTM as in_CREATED_DTTM", \
	"FIL_NO_CHANGE_REC___in_LAST_UPDATED_DTTM as in_LAST_UPDATED_DTTM", \
	"FIL_NO_CHANGE_REC___in_UPDATE_TSTMP as in_UPDATE_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_NO_CHANGE_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr( \
	"EXP_EVAL_VALUES___LOCATION_ID as LOCATION_ID", \
	"EXP_EVAL_VALUES___MSRMNT_ID as MSRMNT_ID", \
	"EXP_EVAL_VALUES___MSRMNT_CODE as MSRMNT_CODE", \
	"EXP_EVAL_VALUES___NAME as NAME", \
	"EXP_EVAL_VALUES___ORIG_MSRMNT_CODE as ORIG_MSRMNT_CODE", \
	"EXP_EVAL_VALUES___ORIG_NAME as ORIG_NAME", \
	"EXP_EVAL_VALUES___STATUS_FLAG as STATUS_FLAG", \
	"EXP_EVAL_VALUES___SYS_CREATED_FLAG as SYS_CREATED_FLAG", \
	"EXP_EVAL_VALUES___UNQ_SEED_ID as UNQ_SEED_ID", \
	"EXP_EVAL_VALUES___SIM_WHSE as SIM_WHSE", \
	"EXP_EVAL_VALUES___MISC_TXT_1 as MISC_TXT_1", \
	"EXP_EVAL_VALUES___MISC_TXT_2 as MISC_TXT_2", \
	"EXP_EVAL_VALUES___MISC_NUM_1 as MISC_NUM_1", \
	"EXP_EVAL_VALUES___MISC_NUM_2 as MISC_NUM_2", \
	"EXP_EVAL_VALUES___USER_ID as USER_ID", \
	"EXP_EVAL_VALUES___VERSION_ID as VERSION_ID", \
	"EXP_EVAL_VALUES___CREATED_DTTM as CREATED_DTTM", \
	"EXP_EVAL_VALUES___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_EVAL_VALUES___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_EVAL_VALUES___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_EVAL_VALUES___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_EVAL_VALUES___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVAL_VALUES___in_MSRMNT_ID as in_MSRMNT_ID") \
	.withColumn('pyspark_data_action', when((in_MSRMNT_ID.isNull()) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_MSRMNT, type TARGET 
# COLUMN COUNT: 22

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_MSRMNT_ID = target.WM_MSRMNT_ID"""
  refined_perf_table = "WM_E_MSRMNT"
  executeMerge(UPD_VALIDATE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_MSRMNT", "WM_E_MSRMNT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_MSRMNT", "WM_E_MSRMNT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	