#Code converted on 2023-06-26 10:01:45
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
raw_perf_table = f"{raw}.WM_E_ACT_ELM_CRIT_PRE"
refined_perf_table = f"{refine}.WM_E_ACT_ELM_CRIT"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic= ' -- '  #args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE = spark.sql(f"""SELECT
DC_NBR,
ACT_ID,
ELM_ID,
CRIT_ID,
CRIT_VAL_ID,
TIME_ALLOW,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
VERSION_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE_temp = SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE.toDF(*["SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___" + col for col in SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___DC_NBR as int) as DC_NBR", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___ACT_ID as ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___ELM_ID as ELM_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___CRIT_ID as CRIT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___CRIT_VAL_ID as CRIT_VAL_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___TIME_ALLOW as TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_ACT_ELM_CRIT, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_WM_E_ACT_ELM_CRIT = spark.sql(f"""SELECT
LOCATION_ID,
WM_ACT_ID,
WM_ELM_ID,
WM_CRIT_VAL_ID,
TIME_ALLOW,
WM_CRIT_ID,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
WM_USER_ID,
WM_VERSION_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
  WHERE {Del_Logic} 1=0 and 
  DELETE_FLAG=0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 17

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_ACT_ELM_CRIT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp = SQ_Shortcut_to_WM_E_ACT_ELM_CRIT.toDF(*["SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___" + col for col in SQ_Shortcut_to_WM_E_ACT_ELM_CRIT.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_E_ACT_ELM_CRIT = SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp.SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp.SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_ACT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ACT_ID, SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp.SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_ELM_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ELM_ID, SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp.SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_CRIT_VAL_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___CRIT_VAL_ID, SQ_Shortcut_to_WM_E_ACT_ELM_CRIT_temp.SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_CRIT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___CRIT_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ACT_ID as ACT_ID", \
	"JNR_SITE_PROFILE___ELM_ID as ELM_ID", \
	"JNR_SITE_PROFILE___CRIT_ID as CRIT_ID", \
	"JNR_SITE_PROFILE___CRIT_VAL_ID as CRIT_VAL_ID", \
	"JNR_SITE_PROFILE___TIME_ALLOW as TIME_ALLOW", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_ACT_ID as in_WM_ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_ELM_ID as in_WM_ELM_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_CRIT_VAL_ID as in_WM_CRIT_VAL_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___TIME_ALLOW as in_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_USER_ID as in_WM_USER_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_CRIT_ID as in_WM_CRIT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___MISC_TXT_1 as in_MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___MISC_TXT_2 as in_MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___MISC_NUM_1 as in_MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___MISC_NUM_2 as in_MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___WM_VERSION_ID as in_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___DELETE_FLAG as in_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_CRIT___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_ACT_ELM_CRIT_temp = JNR_WM_E_ACT_ELM_CRIT.toDF(*["JNR_WM_E_ACT_ELM_CRIT___" + col for col in JNR_WM_E_ACT_ELM_CRIT.columns])

FIL_NO_CHANGE_REC = JNR_WM_E_ACT_ELM_CRIT_temp.selectExpr( \
	"JNR_WM_E_ACT_ELM_CRIT___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___ACT_ID as ACT_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___ELM_ID as ELM_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___CRIT_ID as CRIT_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___CRIT_VAL_ID as CRIT_VAL_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___TIME_ALLOW as TIME_ALLOW", \
	"JNR_WM_E_ACT_ELM_CRIT___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_ACT_ELM_CRIT___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_ACT_ELM_CRIT___USER_ID as USER_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_ACT_ELM_CRIT___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_ACT_ELM_CRIT___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_ACT_ELM_CRIT___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_ACT_ELM_CRIT___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_WM_E_ACT_ELM_CRIT___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_ACT_ID as in_WM_ACT_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_ELM_ID as in_WM_ELM_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_CRIT_VAL_ID as in_WM_CRIT_VAL_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_TIME_ALLOW as in_TIME_ALLOW", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_USER_ID as in_WM_USER_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_CRIT_ID as in_WM_CRIT_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_MISC_TXT_1 as in_MISC_TXT_1", \
	"JNR_WM_E_ACT_ELM_CRIT___in_MISC_TXT_2 as in_MISC_TXT_2", \
	"JNR_WM_E_ACT_ELM_CRIT___in_MISC_NUM_1 as in_MISC_NUM_1", \
	"JNR_WM_E_ACT_ELM_CRIT___in_MISC_NUM_2 as in_MISC_NUM_2", \
	"JNR_WM_E_ACT_ELM_CRIT___in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"JNR_WM_E_ACT_ELM_CRIT___in_DELETE_FLAG as in_DELETE_FLAG", \
	"JNR_WM_E_ACT_ELM_CRIT___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("( ACT_ID is Null OR ELM_ID is Null OR CRIT_ID is Null OR CRIT_VAL_ID is Null ) OR ( in_WM_ACT_ID is Null OR in_WM_ELM_ID is Null OR in_WM_CRIT_VAL_ID is Null OR in_WM_CRIT_ID is Null ) OR \
            ( (  in_WM_ACT_ID is NOT Null OR  in_WM_ELM_ID is NOT Null OR  in_WM_CRIT_VAL_ID is NOT Null OR  in_WM_CRIT_ID is NOT Null ) \
             AND ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(in_WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(in_WM_MOD_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_EVALUATE, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns]) \
.withColumn("FIL_NO_CHANGE_REC___v_CREATE_DATE_TIME", expr("""IF(FIL_NO_CHANGE_REC___CREATE_DATE_TIME IS NULL, date'1900-01-01', FIL_NO_CHANGE_REC___CREATE_DATE_TIME)""")) \
	.withColumn("FIL_NO_CHANGE_REC___v_MOD_DATE_TIME", expr("""IF(FIL_NO_CHANGE_REC___MOD_DATE_TIME IS NULL, date'1900-01-01', FIL_NO_CHANGE_REC___MOD_DATE_TIME)""")) \
	.withColumn("FIL_NO_CHANGE_REC___v_in_WM_CREATE_TSTMP", expr("""IF(FIL_NO_CHANGE_REC___in_WM_CREATE_TSTMP IS NULL, date'1900-01-01', FIL_NO_CHANGE_REC___in_WM_CREATE_TSTMP)""")) \
	.withColumn("FIL_NO_CHANGE_REC___v_in_WM_MOD_TSTMP", expr("""IF(FIL_NO_CHANGE_REC___in_WM_MOD_TSTMP IS NULL, date'1900-01-01', FIL_NO_CHANGE_REC___in_WM_MOD_TSTMP)"""))

EXP_EVALUATE = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___ACT_ID as ACT_ID", \
	"FIL_NO_CHANGE_REC___ELM_ID as ELM_ID", \
	"FIL_NO_CHANGE_REC___CRIT_ID as CRIT_ID", \
	"FIL_NO_CHANGE_REC___CRIT_VAL_ID as CRIT_VAL_ID", \
	"FIL_NO_CHANGE_REC___TIME_ALLOW as TIME_ALLOW", \
	"FIL_NO_CHANGE_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___USER_ID as USER_ID", \
	"FIL_NO_CHANGE_REC___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___VERSION_ID as VERSION_ID", \
	"FIL_NO_CHANGE_REC___LOAD_TSTMP as LOAD_TSTMP2", \
	"FIL_NO_CHANGE_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_NO_CHANGE_REC___in_WM_ACT_ID as in_WM_ACT_ID", \
	"FIL_NO_CHANGE_REC___in_WM_ELM_ID as in_WM_ELM_ID", \
	"FIL_NO_CHANGE_REC___in_WM_CRIT_VAL_ID as in_WM_CRIT_VAL_ID", \
	"FIL_NO_CHANGE_REC___in_TIME_ALLOW as in_TIME_ALLOW", \
	"FIL_NO_CHANGE_REC___in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_USER_ID as in_WM_USER_ID", \
	"FIL_NO_CHANGE_REC___in_WM_CRIT_ID as in_WM_CRIT_ID", \
	"FIL_NO_CHANGE_REC___in_MISC_TXT_1 as in_MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___in_MISC_TXT_2 as in_MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___in_MISC_NUM_1 as in_MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___in_MISC_NUM_2 as in_MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"IF(FIL_NO_CHANGE_REC___ACT_ID IS NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"IF(FIL_NO_CHANGE_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	f"IF(FIL_NO_CHANGE_REC___ACT_ID IS NOT NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NULL, 'INSERT', IF(FIL_NO_CHANGE_REC___ACT_ID IS NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NOT NULL AND ( FIL_NO_CHANGE_REC___v_in_WM_CREATE_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_NO_CHANGE_REC___v_in_WM_MOD_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) ), 'DELETE', IF(FIL_NO_CHANGE_REC___ACT_ID IS NOT NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NOT NULL AND ( FIL_NO_CHANGE_REC___v_in_WM_CREATE_TSTMP <> FIL_NO_CHANGE_REC___v_CREATE_DATE_TIME OR FIL_NO_CHANGE_REC___v_in_WM_MOD_TSTMP <> FIL_NO_CHANGE_REC___v_MOD_DATE_TIME ), 'UPDATE', NULL))) as LOAD_FLAG" \
)

# COMMAND ----------
# Processing node RTR_INS_UP_DEL, type ROUTER 
# COLUMN COUNT: 33


# Creating output dataframe for RTR_INS_UP_DEL, output group DEL
RTR_INS_UP_DEL_DEL = EXP_EVALUATE.select(EXP_EVALUATE.sys_row_id.alias('sys_row_id'), \
	EXP_EVALUATE.LOCATION_ID.alias('LOCATION_ID3'), \
	EXP_EVALUATE.ACT_ID.alias('ACT_ID3'), \
	EXP_EVALUATE.ELM_ID.alias('ELM_ID3'), \
	EXP_EVALUATE.CRIT_ID.alias('CRIT_ID3'), \
	EXP_EVALUATE.CRIT_VAL_ID.alias('CRIT_VAL_ID3'), \
	EXP_EVALUATE.TIME_ALLOW.alias('TIME_ALLOW3'), \
	EXP_EVALUATE.CREATE_DATE_TIME.alias('CREATE_DATE_TIME3'), \
	EXP_EVALUATE.MOD_DATE_TIME.alias('MOD_DATE_TIME3'), \
	EXP_EVALUATE.USER_ID.alias('USER_ID3'), \
	EXP_EVALUATE.MISC_TXT_1.alias('MISC_TXT_13'), \
	EXP_EVALUATE.MISC_TXT_2.alias('MISC_TXT_23'), \
	EXP_EVALUATE.MISC_NUM_1.alias('MISC_NUM_13'), \
	EXP_EVALUATE.MISC_NUM_2.alias('MISC_NUM_23'), \
	EXP_EVALUATE.VERSION_ID.alias('VERSION_ID3'), \
	EXP_EVALUATE.LOAD_TSTMP2.alias('LOAD_TSTMP23'), \
	EXP_EVALUATE.in_LOCATION_ID.alias('in_LOCATION_ID3'), \
	EXP_EVALUATE.in_WM_ACT_ID.alias('in_WM_ACT_ID3'), \
	EXP_EVALUATE.in_WM_ELM_ID.alias('in_WM_ELM_ID3'), \
	EXP_EVALUATE.in_WM_CRIT_VAL_ID.alias('in_WM_CRIT_VAL_ID3'), \
	EXP_EVALUATE.in_TIME_ALLOW.alias('in_TIME_ALLOW3'), \
	EXP_EVALUATE.in_WM_CREATE_TSTMP.alias('in_WM_CREATE_TSTMP3'), \
	EXP_EVALUATE.in_WM_MOD_TSTMP.alias('in_WM_MOD_TSTMP3'), \
	EXP_EVALUATE.in_WM_USER_ID.alias('in_WM_USER_ID3'), \
	EXP_EVALUATE.in_WM_CRIT_ID.alias('in_WM_CRIT_ID3'), \
	EXP_EVALUATE.in_MISC_TXT_1.alias('in_MISC_TXT_13'), \
	EXP_EVALUATE.in_MISC_TXT_2.alias('in_MISC_TXT_23'), \
	EXP_EVALUATE.in_MISC_NUM_1.alias('in_MISC_NUM_13'), \
	EXP_EVALUATE.in_MISC_NUM_2.alias('in_MISC_NUM_23'), \
	EXP_EVALUATE.in_WM_VERSION_ID.alias('in_WM_VERSION_ID3'), \
	EXP_EVALUATE.DELETE_FLAG.alias('DELETE_FLAG3'), \
	EXP_EVALUATE.LOAD_TSTMP.alias('LOAD_TSTMP4'), \
	EXP_EVALUATE.UPDATE_TSTMP.alias('UPDATE_TSTMP3'), \
	EXP_EVALUATE.LOAD_FLAG.alias('LOAD_FLAG3')).filter("LOAD_FLAG = 'DELETE'")

# Creating output dataframe for RTR_INS_UP_DEL, output group INS_UPD
RTR_INS_UP_DEL_INS_UPD = EXP_EVALUATE.select(EXP_EVALUATE.sys_row_id.alias('sys_row_id'), \
	EXP_EVALUATE.LOCATION_ID.alias('LOCATION_ID1'), \
	EXP_EVALUATE.ACT_ID.alias('ACT_ID1'), \
	EXP_EVALUATE.ELM_ID.alias('ELM_ID1'), \
	EXP_EVALUATE.CRIT_ID.alias('CRIT_ID1'), \
	EXP_EVALUATE.CRIT_VAL_ID.alias('CRIT_VAL_ID1'), \
	EXP_EVALUATE.TIME_ALLOW.alias('TIME_ALLOW1'), \
	EXP_EVALUATE.CREATE_DATE_TIME.alias('CREATE_DATE_TIME1'), \
	EXP_EVALUATE.MOD_DATE_TIME.alias('MOD_DATE_TIME1'), \
	EXP_EVALUATE.USER_ID.alias('USER_ID1'), \
	EXP_EVALUATE.MISC_TXT_1.alias('MISC_TXT_11'), \
	EXP_EVALUATE.MISC_TXT_2.alias('MISC_TXT_21'), \
	EXP_EVALUATE.MISC_NUM_1.alias('MISC_NUM_11'), \
	EXP_EVALUATE.MISC_NUM_2.alias('MISC_NUM_21'), \
	EXP_EVALUATE.VERSION_ID.alias('VERSION_ID1'), \
	EXP_EVALUATE.LOAD_TSTMP2.alias('LOAD_TSTMP21'), \
	EXP_EVALUATE.in_LOCATION_ID.alias('in_LOCATION_ID1'), \
	EXP_EVALUATE.in_WM_ACT_ID.alias('in_WM_ACT_ID1'), \
	EXP_EVALUATE.in_WM_ELM_ID.alias('in_WM_ELM_ID1'), \
	EXP_EVALUATE.in_WM_CRIT_VAL_ID.alias('in_WM_CRIT_VAL_ID1'), \
	EXP_EVALUATE.in_TIME_ALLOW.alias('in_TIME_ALLOW1'), \
	EXP_EVALUATE.in_WM_CREATE_TSTMP.alias('in_WM_CREATE_TSTMP1'), \
	EXP_EVALUATE.in_WM_MOD_TSTMP.alias('in_WM_MOD_TSTMP1'), \
	EXP_EVALUATE.in_WM_USER_ID.alias('in_WM_USER_ID1'), \
	EXP_EVALUATE.in_WM_CRIT_ID.alias('in_WM_CRIT_ID1'), \
	EXP_EVALUATE.in_MISC_TXT_1.alias('in_MISC_TXT_11'), \
	EXP_EVALUATE.in_MISC_TXT_2.alias('in_MISC_TXT_21'), \
	EXP_EVALUATE.in_MISC_NUM_1.alias('in_MISC_NUM_11'), \
	EXP_EVALUATE.in_MISC_NUM_2.alias('in_MISC_NUM_21'), \
	EXP_EVALUATE.in_WM_VERSION_ID.alias('in_WM_VERSION_ID1'), \
	EXP_EVALUATE.DELETE_FLAG.alias('DELETE_FLAG1'), \
	EXP_EVALUATE.LOAD_TSTMP.alias('LOAD_TSTMP1'), \
	EXP_EVALUATE.UPDATE_TSTMP.alias('UPDATE_TSTMP1'), \
	EXP_EVALUATE.LOAD_FLAG.alias('LOAD_FLAG1')).filter("LOAD_FLAG = 'INSERT' OR LOAD_FLAG = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_DEL, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UP_DEL_DEL_temp = RTR_INS_UP_DEL_DEL.toDF(*["RTR_INS_UP_DEL_DEL___" + col for col in RTR_INS_UP_DEL_DEL.columns])

UPD_DEL = RTR_INS_UP_DEL_DEL_temp.selectExpr( \
	"RTR_INS_UP_DEL_DEL___in_LOCATION_ID3 as LOCATION_ID3", \
	"RTR_INS_UP_DEL_DEL___in_WM_ACT_ID3 as ACT_ID3", \
	"RTR_INS_UP_DEL_DEL___in_WM_ELM_ID3 as ELM_ID3", \
	"RTR_INS_UP_DEL_DEL___in_WM_CRIT_ID3 as CRIT_ID3", \
	"RTR_INS_UP_DEL_DEL___in_WM_CRIT_VAL_ID3 as CRIT_VAL_ID3", \
	"RTR_INS_UP_DEL_DEL___in_TIME_ALLOW3 as TIME_ALLOW3", \
	"RTR_INS_UP_DEL_DEL___in_WM_CREATE_TSTMP3 as CREATE_DATE_TIME3", \
	"RTR_INS_UP_DEL_DEL___in_WM_MOD_TSTMP3 as MOD_DATE_TIME3", \
	"RTR_INS_UP_DEL_DEL___in_WM_USER_ID3 as USER_ID3", \
	"RTR_INS_UP_DEL_DEL___in_MISC_TXT_13 as MISC_TXT_13", \
	"RTR_INS_UP_DEL_DEL___in_MISC_TXT_23 as MISC_TXT_23", \
	"RTR_INS_UP_DEL_DEL___in_MISC_NUM_13 as MISC_NUM_13", \
	"RTR_INS_UP_DEL_DEL___in_MISC_NUM_23 as MISC_NUM_23", \
	"RTR_INS_UP_DEL_DEL___in_WM_VERSION_ID3 as VERSION_ID3", \
	"RTR_INS_UP_DEL_DEL___DELETE_FLAG3 as DELETE_FLAG3", \
	"RTR_INS_UP_DEL_DEL___LOAD_TSTMP4 as LOAD_TSTMP4", \
	"RTR_INS_UP_DEL_DEL___UPDATE_TSTMP3 as UPDATE_TSTMP3", \
	"RTR_INS_UP_DEL_DEL___LOAD_FLAG3 as LOAD_FLAG3") \
	.withColumn('pyspark_data_action', lit(2))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_ACT_ELM_CRIT2, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_WM_E_ACT_ELM_CRIT2 = UPD_DEL.selectExpr( \
	"CAST(LOCATION_ID3 AS BIGINT) as LOCATION_ID", \
	"CAST(ACT_ID3 AS BIGINT) as WM_ACT_ID", \
	"CAST(ELM_ID3 AS BIGINT) as WM_ELM_ID", \
	"CAST(CRIT_ID3 AS BIGINT) as WM_CRIT_ID", \
	"CAST(CRIT_VAL_ID3 AS BIGINT) as WM_CRIT_VAL_ID", \
	"CAST(NULL AS BIGINT) as TIME_ALLOW", \
	"CAST(NULL AS STRING) as MISC_TXT_1", \
	"CAST(NULL AS STRING) as MISC_TXT_2", \
	"CAST(NULL AS BIGINT) as MISC_NUM_1", \
	"CAST(NULL AS BIGINT) as MISC_NUM_2", \
	"CAST(NULL AS STRING) as WM_USER_ID", \
	"CAST(NULL AS BIGINT) as WM_VERSION_ID", \
	"CAST(NULL AS TIMESTAMP) as WM_CREATE_TSTMP", \
	"CAST(NULL AS TIMESTAMP) as WM_MOD_TSTMP", \
	"CAST(DELETE_FLAG3 AS TINYINT) as DELETE_FLAG", \
	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP" \
)
# Shortcut_to_WM_E_ACT_ELM_CRIT2.write.saveAsTable(f'{raw}.WM_E_ACT_ELM_CRIT')

Shortcut_to_WM_E_ACT_ELM_CRIT2.createOrReplaceTempView('WM_E_ACT_ELM_CRIT_DEL')

spark.sql(f"""
          MERGE INTO {refined_perf_table} trg
          USING WM_E_ACT_ELM_CRIT_DEL src
          ON (src.LOCATION_ID = trg.LOCATION_ID AND src.WM_ACT_ID = trg.WM_ACT_ID AND src.WM_ELM_ID = trg.WM_ELM_ID AND src.WM_CRIT_ID = trg.WM_CRIT_ID AND src.WM_CRIT_VAL_ID = trg.WM_CRIT_VAL_ID )
          WHEN MATCHED THEN UPDATE SET trg.DELETE_FLAG = src.DELETE_FLAG , trg.UPDATE_TSTMP = src.UPDATE_TSTMP
          """)



# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UP_DEL_INS_UPD_temp = RTR_INS_UP_DEL_INS_UPD.toDF(*["RTR_INS_UP_DEL_INS_UPD___" + col for col in RTR_INS_UP_DEL_INS_UPD.columns])

UPD_INS_UPD = RTR_INS_UP_DEL_INS_UPD_temp.selectExpr( \
	"RTR_INS_UP_DEL_INS_UPD___LOCATION_ID1 as LOCATION_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___ACT_ID1 as ACT_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___ELM_ID1 as ELM_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___CRIT_ID1 as CRIT_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___CRIT_VAL_ID1 as CRIT_VAL_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___TIME_ALLOW1 as TIME_ALLOW1", \
	"RTR_INS_UP_DEL_INS_UPD___CREATE_DATE_TIME1 as CREATE_DATE_TIME1", \
	"RTR_INS_UP_DEL_INS_UPD___MOD_DATE_TIME1 as MOD_DATE_TIME1", \
	"RTR_INS_UP_DEL_INS_UPD___USER_ID1 as USER_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___MISC_TXT_11 as MISC_TXT_11", \
	"RTR_INS_UP_DEL_INS_UPD___MISC_TXT_21 as MISC_TXT_21", \
	"RTR_INS_UP_DEL_INS_UPD___MISC_NUM_11 as MISC_NUM_11", \
	"RTR_INS_UP_DEL_INS_UPD___MISC_NUM_21 as MISC_NUM_21", \
	"RTR_INS_UP_DEL_INS_UPD___VERSION_ID1 as VERSION_ID1", \
	"RTR_INS_UP_DEL_INS_UPD___DELETE_FLAG1 as DELETE_FLAG1", \
	"RTR_INS_UP_DEL_INS_UPD___LOAD_TSTMP1 as LOAD_TSTMP1", \
	"RTR_INS_UP_DEL_INS_UPD___UPDATE_TSTMP1 as UPDATE_TSTMP1", \
	"RTR_INS_UP_DEL_INS_UPD___LOAD_FLAG1 as LOAD_FLAG1") \
	.withColumn('pyspark_data_action', when(col('LOAD_FLAG1') ==(lit('INSERT')), lit(0)).when(col('LOAD_FLAG1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_ACT_ELM_CRIT1, type TARGET 
# COLUMN COUNT: 17

Shortcut_to_WM_E_ACT_ELM_CRIT1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(ACT_ID1 AS INT) as WM_ACT_ID",
	"CAST(ELM_ID1 AS INT) as WM_ELM_ID",
	"CAST(CRIT_ID1 AS INT) as WM_CRIT_ID",
	"CAST(CRIT_VAL_ID1 AS INT) as WM_CRIT_VAL_ID",
	"CAST(TIME_ALLOW1 AS DECIMAL(9,4)) as TIME_ALLOW",
	"CAST(MISC_TXT_11 AS STRING) as MISC_TXT_1",
	"CAST(MISC_TXT_21 AS STRING) as MISC_TXT_2",
	"CAST(MISC_NUM_11 AS DECIMAL(20,7)) as MISC_NUM_1",
	"CAST(MISC_NUM_21 AS DECIMAL(20,7)) as MISC_NUM_2",
	"CAST(USER_ID1 AS STRING) as WM_USER_ID",
	"CAST(VERSION_ID1 AS INT) as WM_VERSION_ID",
	"CAST(CREATE_DATE_TIME1 AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME1 AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(DELETE_FLAG1 AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ACT_ID = target.WM_ACT_ID AND source.WM_ELM_ID = target.WM_ELM_ID AND source.WM_CRIT_ID = target.WM_CRIT_ID AND source.WM_CRIT_VAL_ID = target.WM_CRIT_VAL_ID"""
#   refined_perf_table = "WM_E_ACT_ELM_CRIT"
  executeMerge(Shortcut_to_WM_E_ACT_ELM_CRIT1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_ACT_ELM_CRIT", "WM_E_ACT_ELM_CRIT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_ACT_ELM_CRIT", "WM_E_ACT_ELM_CRIT","Failed",str(e), f"{raw}.log_run_details", )
  raise e