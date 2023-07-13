#Code converted on 2023-06-26 10:02:00
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
raw_perf_table = f"{raw}.WM_E_ACT_ELM_PRE"
refined_perf_table = f"{refine}.WM_E_ACT_ELM"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
# Del_Logic= ' -- '  #args.Del_Logic
Del_Logic=' -- '

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_ACT_ELM, type SOURCE 
# COLUMN COUNT: 19

SQ_Shortcut_to_WM_E_ACT_ELM = spark.sql(f"""SELECT
LOCATION_ID,
WM_ACT_ID,
WM_ELM_ID,
WM_SEQ_NBR,
TIME_ALLOW,
WM_THRUPUT_MSRMNT,
WM_AVG_ACT_ID,
WM_AVG_BY,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
WM_USER_ID,
WM_VERSION_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
DELETE_FLAG,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_ACT_ELM_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WM_E_ACT_ELM_PRE = spark.sql(f"""SELECT
DC_NBR,
ACT_ID,
ELM_ID,
TIME_ALLOW,
THRUPUT_MSRMNT,
SEQ_NBR,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
VERSION_ID,
AVG_ACT_ID,
AVG_BY,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_ACT_ELM_PRE_temp = SQ_Shortcut_to_WM_E_ACT_ELM_PRE.toDF(*["SQ_Shortcut_to_WM_E_ACT_ELM_PRE___" + col for col in SQ_Shortcut_to_WM_E_ACT_ELM_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_ACT_ELM_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___DC_NBR as in_DC_NBR", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___ACT_ID as ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___ELM_ID as ELM_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___TIME_ALLOW as TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___SEQ_NBR as SEQ_NBR", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___AVG_ACT_ID as AVG_ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___AVG_BY as AVG_BY", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___LOAD_TSTMP as LOAD_TSTMP").selectExpr( \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_ACT_ELM_PRE___in_DC_NBR as int) as DC_NBR", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___ACT_ID as ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___ELM_ID as ELM_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___TIME_ALLOW as TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___SEQ_NBR as SEQ_NBR", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___AVG_ACT_ID as AVG_ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___AVG_BY as AVG_BY", \
	"SQ_Shortcut_to_WM_E_ACT_ELM_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 19

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_ACT_ELM, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 35

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_E_ACT_ELM_temp = SQ_Shortcut_to_WM_E_ACT_ELM.toDF(*["SQ_Shortcut_to_WM_E_ACT_ELM___" + col for col in SQ_Shortcut_to_WM_E_ACT_ELM.columns])

JNR_WM_E_ACT_ELM = SQ_Shortcut_to_WM_E_ACT_ELM_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_ACT_ELM_temp.SQ_Shortcut_to_WM_E_ACT_ELM___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_ACT_ELM_temp.SQ_Shortcut_to_WM_E_ACT_ELM___WM_ACT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ACT_ID, SQ_Shortcut_to_WM_E_ACT_ELM_temp.SQ_Shortcut_to_WM_E_ACT_ELM___WM_ELM_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ELM_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ACT_ID as ACT_ID", \
	"JNR_SITE_PROFILE___ELM_ID as ELM_ID", \
	"JNR_SITE_PROFILE___TIME_ALLOW as TIME_ALLOW", \
	"JNR_SITE_PROFILE___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"JNR_SITE_PROFILE___SEQ_NBR as SEQ_NBR", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___AVG_ACT_ID as AVG_ACT_ID", \
	"JNR_SITE_PROFILE___AVG_BY as AVG_BY", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_ACT_ID as in_WM_ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_ELM_ID as in_WM_ELM_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___TIME_ALLOW as in_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_THRUPUT_MSRMNT as in_WM_THRUPUT_MSRMNT", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_SEQ_NBR as in_WM_SEQ_NBR", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_USER_ID as in_WM_USER_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___MISC_TXT_1 as in_MISC_TXT_11", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___MISC_TXT_2 as in_MISC_TXT_21", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___MISC_NUM_1 as in_MISC_NUM_11", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___MISC_NUM_2 as in_MISC_NUM_21", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_VERSION_ID as in_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_AVG_ACT_ID as in_WM_AVG_ACT_ID", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___WM_AVG_BY as in_WM_AVG_BY", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___DELETE_FLAG as in_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___UPDATE_TSTMP as in_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_ACT_ELM___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 35

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_ACT_ELM_temp = JNR_WM_E_ACT_ELM.toDF(*["JNR_WM_E_ACT_ELM___" + col for col in JNR_WM_E_ACT_ELM.columns])

FIL_NO_CHANGE_REC = JNR_WM_E_ACT_ELM_temp.selectExpr( \
	"JNR_WM_E_ACT_ELM___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_ACT_ELM___ACT_ID as ACT_ID", \
	"JNR_WM_E_ACT_ELM___ELM_ID as ELM_ID", \
	"JNR_WM_E_ACT_ELM___TIME_ALLOW as TIME_ALLOW", \
	"JNR_WM_E_ACT_ELM___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"JNR_WM_E_ACT_ELM___SEQ_NBR as SEQ_NBR", \
	"JNR_WM_E_ACT_ELM___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_ACT_ELM___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_ACT_ELM___USER_ID as USER_ID", \
	"JNR_WM_E_ACT_ELM___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_ACT_ELM___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_ACT_ELM___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_ACT_ELM___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_ACT_ELM___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_ACT_ELM___AVG_ACT_ID as AVG_ACT_ID", \
	"JNR_WM_E_ACT_ELM___AVG_BY as AVG_BY", \
	"JNR_WM_E_ACT_ELM___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_E_ACT_ELM___in_WM_ACT_ID as in_WM_ACT_ID", \
	"JNR_WM_E_ACT_ELM___in_WM_ELM_ID as in_WM_ELM_ID", \
	"JNR_WM_E_ACT_ELM___in_TIME_ALLOW as in_TIME_ALLOW", \
	"JNR_WM_E_ACT_ELM___in_WM_THRUPUT_MSRMNT as in_WM_THRUPUT_MSRMNT", \
	"JNR_WM_E_ACT_ELM___in_WM_SEQ_NBR as in_WM_SEQ_NBR", \
	"JNR_WM_E_ACT_ELM___in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"JNR_WM_E_ACT_ELM___in_WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"JNR_WM_E_ACT_ELM___in_WM_USER_ID as in_WM_USER_ID", \
	"JNR_WM_E_ACT_ELM___in_MISC_TXT_11 as in_MISC_TXT_11", \
	"JNR_WM_E_ACT_ELM___in_MISC_TXT_21 as in_MISC_TXT_21", \
	"JNR_WM_E_ACT_ELM___in_MISC_NUM_11 as in_MISC_NUM_11", \
	"JNR_WM_E_ACT_ELM___in_MISC_NUM_21 as in_MISC_NUM_21", \
	"JNR_WM_E_ACT_ELM___in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"JNR_WM_E_ACT_ELM___in_WM_AVG_ACT_ID as in_WM_AVG_ACT_ID", \
	"JNR_WM_E_ACT_ELM___in_WM_AVG_BY as in_WM_AVG_BY", \
	"JNR_WM_E_ACT_ELM___in_DELETE_FLAG as in_DELETE_FLAG", \
	"JNR_WM_E_ACT_ELM___in_UPDATE_TSTMP as in_UPDATE_TSTMP", \
	"JNR_WM_E_ACT_ELM___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("( ACT_ID IS NULL OR ELM_ID IS NULL ) OR ( in_WM_ACT_ID IS NULL OR in_WM_ELM_ID IS NULL ) OR ( ( in_WM_ACT_ID IS NUT NULL OR  in_WM_ELM_ID IS NUT NULL ) \
             AND ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(in_WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(in_WM_MOD_TSTMP, date'1900-01-01') ) )").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_EVALUATE, type EXPRESSION 
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns]) \
.withColumn("v_CREATE_DATE_TIME", expr("""IF(CREATE_DATE_TIME IS NULL, date'1900-01-01', CREATE_DATE_TIME)""")) \
	.withColumn("v_MOD_DATE_TIME", expr("""IF(MOD_DATE_TIME IS NULL, date'1900-01-01', MOD_DATE_TIME)""")) \
	.withColumn("v_in_WM_CREATE_TSTMP", expr("""IF(in_WM_CREATE_TSTMP IS NULL, date'1900-01-01', in_WM_CREATE_TSTMP)""")) \
	.withColumn("v_in_WM_MOD_TSTMP", expr("""IF(in_WM_MOD_TSTMP IS NULL, date'1900-01-01', in_WM_MOD_TSTMP)"""))
             
EXP_EVALUATE = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___ACT_ID as ACT_ID", \
	"FIL_NO_CHANGE_REC___ELM_ID as ELM_ID", \
	"FIL_NO_CHANGE_REC___TIME_ALLOW as TIME_ALLOW", \
	"FIL_NO_CHANGE_REC___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"FIL_NO_CHANGE_REC___SEQ_NBR as SEQ_NBR", \
	"FIL_NO_CHANGE_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_NO_CHANGE_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_NO_CHANGE_REC___USER_ID as USER_ID", \
	"FIL_NO_CHANGE_REC___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_NO_CHANGE_REC___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_NO_CHANGE_REC___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_NO_CHANGE_REC___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_NO_CHANGE_REC___VERSION_ID as VERSION_ID", \
	"FIL_NO_CHANGE_REC___AVG_ACT_ID as AVG_ACT_ID", \
	"FIL_NO_CHANGE_REC___AVG_BY as AVG_BY", \
	"FIL_NO_CHANGE_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_NO_CHANGE_REC___in_WM_ACT_ID as in_WM_ACT_ID", \
	"FIL_NO_CHANGE_REC___in_WM_ELM_ID as in_WM_ELM_ID", \
	"FIL_NO_CHANGE_REC___in_TIME_ALLOW as in_TIME_ALLOW", \
	"FIL_NO_CHANGE_REC___in_WM_THRUPUT_MSRMNT as in_WM_THRUPUT_MSRMNT", \
	"FIL_NO_CHANGE_REC___in_WM_SEQ_NBR as in_WM_SEQ_NBR", \
	"FIL_NO_CHANGE_REC___in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_USER_ID as in_WM_USER_ID", \
	"FIL_NO_CHANGE_REC___in_MISC_TXT_11 as in_MISC_TXT_11", \
	"FIL_NO_CHANGE_REC___in_MISC_TXT_21 as in_MISC_TXT_21", \
	"FIL_NO_CHANGE_REC___in_MISC_NUM_11 as in_MISC_NUM_11", \
	"FIL_NO_CHANGE_REC___in_MISC_NUM_21 as in_MISC_NUM_21", \
	"FIL_NO_CHANGE_REC___in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"FIL_NO_CHANGE_REC___in_WM_AVG_ACT_ID as in_WM_AVG_ACT_ID", \
	"FIL_NO_CHANGE_REC___in_WM_AVG_BY as in_WM_AVG_BY", \
	"IF(FIL_NO_CHANGE_REC___ACT_ID IS NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_NO_CHANGE_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_NO_CHANGE_REC___ACT_ID IS NOT NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NULL, 'INSERT', IF(FIL_NO_CHANGE_REC___ACT_ID IS NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NOT NULL AND ( FIL_NO_CHANGE_REC___v_in_WM_CREATE_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_NO_CHANGE_REC___v_in_WM_MOD_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) ), 'DELETE', IF(FIL_NO_CHANGE_REC___ACT_ID IS NOT NULL AND FIL_NO_CHANGE_REC___in_WM_ACT_ID IS NOT NULL AND ( FIL_NO_CHANGE_REC___v_in_WM_CREATE_TSTMP <> FIL_NO_CHANGE_REC___v_CREATE_DATE_TIME OR FIL_NO_CHANGE_REC___v_in_WM_MOD_TSTMP <> FIL_NO_CHANGE_REC___v_MOD_DATE_TIME ), 'UPDATE', NULL))) as LOAD_FLAG" \
)

# COMMAND ----------
# Processing node RTR_INS_UPD_DEL, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 39


# Creating output dataframe for RTR_INS_UPD_DEL, output group DEL
RTR_INS_UPD_DEL_DEL = EXP_EVALUATE.selectExpr( \
	"EXP_EVALUATE.LOCATION_ID as LOCATION_ID", \
	"EXP_EVALUATE.ACT_ID as ACT_ID", \
	"EXP_EVALUATE.ELM_ID as ELM_ID", \
	"EXP_EVALUATE.TIME_ALLOW as TIME_ALLOW", \
	"EXP_EVALUATE.THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"EXP_EVALUATE.SEQ_NBR as SEQ_NBR", \
	"EXP_EVALUATE.CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_EVALUATE.MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_EVALUATE.USER_ID as USER_ID", \
	"EXP_EVALUATE.MISC_TXT_1 as MISC_TXT_1", \
	"EXP_EVALUATE.MISC_TXT_2 as MISC_TXT_2", \
	"EXP_EVALUATE.MISC_NUM_1 as MISC_NUM_1", \
	"EXP_EVALUATE.MISC_NUM_2 as MISC_NUM_2", \
	"EXP_EVALUATE.VERSION_ID as VERSION_ID", \
	"EXP_EVALUATE.AVG_ACT_ID as AVG_ACT_ID", \
	"EXP_EVALUATE.AVG_BY as AVG_BY", \
	"EXP_EVALUATE.in_LOCATION_ID as in_LOCATION_ID", \
	"EXP_EVALUATE.in_WM_ACT_ID as in_WM_ACT_ID", \
	"EXP_EVALUATE.in_WM_ELM_ID as in_WM_ELM_ID", \
	"EXP_EVALUATE.in_TIME_ALLOW as in_TIME_ALLOW", \
	"EXP_EVALUATE.in_WM_THRUPUT_MSRMNT as in_WM_THRUPUT_MSRMNT", \
	"EXP_EVALUATE.in_WM_SEQ_NBR as in_WM_SEQ_NBR", \
	"EXP_EVALUATE.in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"EXP_EVALUATE.in_WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"EXP_EVALUATE.in_WM_USER_ID as in_WM_USER_ID", \
	"EXP_EVALUATE.in_MISC_TXT_11 as in_MISC_TXT_11", \
	"EXP_EVALUATE.in_MISC_TXT_21 as in_MISC_TXT_21", \
	"EXP_EVALUATE.in_MISC_NUM_11 as in_MISC_NUM_11", \
	"EXP_EVALUATE.in_MISC_NUM_21 as in_MISC_NUM_21", \
	"EXP_EVALUATE.in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"EXP_EVALUATE.in_WM_AVG_ACT_ID as in_WM_AVG_ACT_ID", \
	"EXP_EVALUATE.in_WM_AVG_BY as in_WM_AVG_BY", \
	"EXP_EVALUATE.DELETE_FLAG as DELETE_FLAG", \
	"EXP_EVALUATE.UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_EVALUATE.LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVALUATE.LOAD_FLAG as LOAD_FLAG") \
	.withColumn('in_DELETE_FLAG', lit(None)) \
	.withColumn('in_UPDATE_TSTMP', lit(None)) \
	.withColumn('in_LOAD_TSTMP', lit(None)) \
	.select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID3'), \
	col('ACT_ID').alias('ACT_ID3'), \
	col('ELM_ID').alias('ELM_ID3'), \
	col('TIME_ALLOW').alias('TIME_ALLOW3'), \
	col('THRUPUT_MSRMNT').alias('THRUPUT_MSRMNT3'), \
	col('SEQ_NBR').alias('SEQ_NBR3'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME3'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME3'), \
	col('USER_ID').alias('USER_ID3'), \
	col('MISC_TXT_1').alias('MISC_TXT_13'), \
	col('MISC_TXT_2').alias('MISC_TXT_23'), \
	col('MISC_NUM_1').alias('MISC_NUM_13'), \
	col('MISC_NUM_2').alias('MISC_NUM_23'), \
	col('VERSION_ID').alias('VERSION_ID3'), \
	col('AVG_ACT_ID').alias('AVG_ACT_ID3'), \
	col('AVG_BY').alias('AVG_BY3'), \
	col('in_LOCATION_ID').alias('in_LOCATION_ID3'), \
	col('in_WM_ACT_ID').alias('in_WM_ACT_ID3'), \
	col('in_WM_ELM_ID').alias('in_WM_ELM_ID3'), \
	col('in_TIME_ALLOW').alias('in_TIME_ALLOW3'), \
	col('in_WM_THRUPUT_MSRMNT').alias('in_WM_THRUPUT_MSRMNT3'), \
	col('in_WM_SEQ_NBR').alias('in_WM_SEQ_NBR3'), \
	col('in_WM_CREATE_TSTMP').alias('in_WM_CREATE_TSTMP3'), \
	col('in_WM_MOD_TSTMP').alias('in_WM_MOD_TSTMP3'), \
	col('in_WM_USER_ID').alias('in_WM_USER_ID3'), \
	col('in_MISC_TXT_11').alias('in_MISC_TXT_113'), \
	col('in_MISC_TXT_21').alias('in_MISC_TXT_213'), \
	col('in_MISC_NUM_11').alias('in_MISC_NUM_113'), \
	col('in_MISC_NUM_21').alias('in_MISC_NUM_213'), \
	col('in_WM_VERSION_ID').alias('in_WM_VERSION_ID3'), \
	col('in_WM_AVG_ACT_ID').alias('in_WM_AVG_ACT_ID3'), \
	col('in_WM_AVG_BY').alias('in_WM_AVG_BY3'), \
	col('in_DELETE_FLAG').alias('in_DELETE_FLAG3'), \
	col('in_UPDATE_TSTMP').alias('in_UPDATE_TSTMP3'), \
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP3'), \
	col('DELETE_FLAG').alias('DELETE_FLAG3'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP3'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP3'), \
	col('LOAD_FLAG').alias('LOAD_FLAG3')).filter("LOAD_FLAG='DELETE'")

# Creating output dataframe for RTR_INS_UPD_DEL, output group INS_UPD
RTR_INS_UPD_DEL_INS_UPD = EXP_EVALUATE.selectExpr( \
	"EXP_EVALUATE.LOCATION_ID as LOCATION_ID", \
	"EXP_EVALUATE.ACT_ID as ACT_ID", \
	"EXP_EVALUATE.ELM_ID as ELM_ID", \
	"EXP_EVALUATE.TIME_ALLOW as TIME_ALLOW", \
	"EXP_EVALUATE.THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"EXP_EVALUATE.SEQ_NBR as SEQ_NBR", \
	"EXP_EVALUATE.CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_EVALUATE.MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_EVALUATE.USER_ID as USER_ID", \
	"EXP_EVALUATE.MISC_TXT_1 as MISC_TXT_1", \
	"EXP_EVALUATE.MISC_TXT_2 as MISC_TXT_2", \
	"EXP_EVALUATE.MISC_NUM_1 as MISC_NUM_1", \
	"EXP_EVALUATE.MISC_NUM_2 as MISC_NUM_2", \
	"EXP_EVALUATE.VERSION_ID as VERSION_ID", \
	"EXP_EVALUATE.AVG_ACT_ID as AVG_ACT_ID", \
	"EXP_EVALUATE.AVG_BY as AVG_BY", \
	"EXP_EVALUATE.in_LOCATION_ID as in_LOCATION_ID", \
	"EXP_EVALUATE.in_WM_ACT_ID as in_WM_ACT_ID", \
	"EXP_EVALUATE.in_WM_ELM_ID as in_WM_ELM_ID", \
	"EXP_EVALUATE.in_TIME_ALLOW as in_TIME_ALLOW", \
	"EXP_EVALUATE.in_WM_THRUPUT_MSRMNT as in_WM_THRUPUT_MSRMNT", \
	"EXP_EVALUATE.in_WM_SEQ_NBR as in_WM_SEQ_NBR", \
	"EXP_EVALUATE.in_WM_CREATE_TSTMP as in_WM_CREATE_TSTMP", \
	"EXP_EVALUATE.in_WM_MOD_TSTMP as in_WM_MOD_TSTMP", \
	"EXP_EVALUATE.in_WM_USER_ID as in_WM_USER_ID", \
	"EXP_EVALUATE.in_MISC_TXT_11 as in_MISC_TXT_11", \
	"EXP_EVALUATE.in_MISC_TXT_21 as in_MISC_TXT_21", \
	"EXP_EVALUATE.in_MISC_NUM_11 as in_MISC_NUM_11", \
	"EXP_EVALUATE.in_MISC_NUM_21 as in_MISC_NUM_21", \
	"EXP_EVALUATE.in_WM_VERSION_ID as in_WM_VERSION_ID", \
	"EXP_EVALUATE.in_WM_AVG_ACT_ID as in_WM_AVG_ACT_ID", \
	"EXP_EVALUATE.in_WM_AVG_BY as in_WM_AVG_BY", \
	"EXP_EVALUATE.DELETE_FLAG as DELETE_FLAG", \
	"EXP_EVALUATE.UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_EVALUATE.LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVALUATE.LOAD_FLAG as LOAD_FLAG") \
	.withColumn('in_DELETE_FLAG', lit(None)) \
	.withColumn('in_UPDATE_TSTMP', lit(None)) \
	.withColumn('in_LOAD_TSTMP', lit(None)) \
	.select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID1'), \
	col('ACT_ID').alias('ACT_ID1'), \
	col('ELM_ID').alias('ELM_ID1'), \
	col('TIME_ALLOW').alias('TIME_ALLOW1'), \
	col('THRUPUT_MSRMNT').alias('THRUPUT_MSRMNT1'), \
	col('SEQ_NBR').alias('SEQ_NBR1'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME1'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME1'), \
	col('USER_ID').alias('USER_ID1'), \
	col('MISC_TXT_1').alias('MISC_TXT_11'), \
	col('MISC_TXT_2').alias('MISC_TXT_21'), \
	col('MISC_NUM_1').alias('MISC_NUM_11'), \
	col('MISC_NUM_2').alias('MISC_NUM_21'), \
	col('VERSION_ID').alias('VERSION_ID1'), \
	col('AVG_ACT_ID').alias('AVG_ACT_ID1'), \
	col('AVG_BY').alias('AVG_BY1'), \
	col('in_LOCATION_ID').alias('in_LOCATION_ID1'), \
	col('in_WM_ACT_ID').alias('in_WM_ACT_ID1'), \
	col('in_WM_ELM_ID').alias('in_WM_ELM_ID1'), \
	col('in_TIME_ALLOW').alias('in_TIME_ALLOW1'), \
	col('in_WM_THRUPUT_MSRMNT').alias('in_WM_THRUPUT_MSRMNT1'), \
	col('in_WM_SEQ_NBR').alias('in_WM_SEQ_NBR1'), \
	col('in_WM_CREATE_TSTMP').alias('in_WM_CREATE_TSTMP1'), \
	col('in_WM_MOD_TSTMP').alias('in_WM_MOD_TSTMP1'), \
	col('in_WM_USER_ID').alias('in_WM_USER_ID1'), \
	col('in_MISC_TXT_11').alias('in_MISC_TXT_111'), \
	col('in_MISC_TXT_21').alias('in_MISC_TXT_211'), \
	col('in_MISC_NUM_11').alias('in_MISC_NUM_111'), \
	col('in_MISC_NUM_21').alias('in_MISC_NUM_211'), \
	col('in_WM_VERSION_ID').alias('in_WM_VERSION_ID1'), \
	col('in_WM_AVG_ACT_ID').alias('in_WM_AVG_ACT_ID1'), \
	col('in_WM_AVG_BY').alias('in_WM_AVG_BY1'), \
	col('in_DELETE_FLAG').alias('in_DELETE_FLAG1'), \
	col('in_UPDATE_TSTMP').alias('in_UPDATE_TSTMP1'), \
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP1'), \
	col('DELETE_FLAG').alias('DELETE_FLAG1'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP1'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP1'), \
	col('LOAD_FLAG').alias('LOAD_FLAG1')).filter("LOAD_FLAG ='INSERT' OR LOAD_FLAG = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_DEL, type UPDATE_STRATEGY 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_DEL_temp = RTR_INS_UPD_DEL_DEL.toDF(*["RTR_INS_UPD_DEL_DEL___" + col for col in RTR_INS_UPD_DEL_DEL.columns])

UPD_DEL = RTR_INS_UPD_DEL_DEL_temp.selectExpr( \
	"RTR_INS_UPD_DEL_DEL___in_LOCATION_ID3 as in_LOCATION_ID3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_ACT_ID3 as in_WM_ACT_ID3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_ELM_ID3 as in_WM_ELM_ID3", \
	"RTR_INS_UPD_DEL_DEL___in_TIME_ALLOW3 as in_TIME_ALLOW3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_THRUPUT_MSRMNT3 as in_WM_THRUPUT_MSRMNT3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_SEQ_NBR3 as in_WM_SEQ_NBR3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_CREATE_TSTMP3 as in_WM_CREATE_TSTMP3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_MOD_TSTMP3 as in_WM_MOD_TSTMP3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_USER_ID3 as in_WM_USER_ID3", \
	"RTR_INS_UPD_DEL_DEL___in_MISC_TXT_113 as in_MISC_TXT_113", \
	"RTR_INS_UPD_DEL_DEL___in_MISC_TXT_213 as in_MISC_TXT_213", \
	"RTR_INS_UPD_DEL_DEL___in_MISC_NUM_113 as in_MISC_NUM_113", \
	"RTR_INS_UPD_DEL_DEL___in_MISC_NUM_213 as in_MISC_NUM_213", \
	"RTR_INS_UPD_DEL_DEL___in_WM_VERSION_ID3 as in_WM_VERSION_ID3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_AVG_ACT_ID3 as in_WM_AVG_ACT_ID3", \
	"RTR_INS_UPD_DEL_DEL___in_WM_AVG_BY3 as in_WM_AVG_BY3", \
	"RTR_INS_UPD_DEL_DEL___DELETE_FLAG3 as DELETE_FLAG3", \
	"RTR_INS_UPD_DEL_DEL___UPDATE_TSTMP3 as UPDATE_TSTMP3", \
	"RTR_INS_UPD_DEL_DEL___LOAD_TSTMP3 as LOAD_TSTMP3", \
	"RTR_INS_UPD_DEL_DEL___LOAD_FLAG3 as LOAD_FLAG3") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_INS_UPD_temp = RTR_INS_UPD_DEL_INS_UPD.toDF(*["RTR_INS_UPD_DEL_INS_UPD___" + col for col in RTR_INS_UPD_DEL_INS_UPD.columns])

UPD_INS_UPD = RTR_INS_UPD_DEL_INS_UPD_temp.selectExpr( \
	"RTR_INS_UPD_DEL_INS_UPD___LOCATION_ID1 as LOCATION_ID1", \
	"RTR_INS_UPD_DEL_INS_UPD___ACT_ID1 as ACT_ID1", \
	"RTR_INS_UPD_DEL_INS_UPD___ELM_ID1 as ELM_ID1", \
	"RTR_INS_UPD_DEL_INS_UPD___TIME_ALLOW1 as TIME_ALLOW1", \
	"RTR_INS_UPD_DEL_INS_UPD___THRUPUT_MSRMNT1 as THRUPUT_MSRMNT1", \
	"RTR_INS_UPD_DEL_INS_UPD___SEQ_NBR1 as SEQ_NBR1", \
	"RTR_INS_UPD_DEL_INS_UPD___CREATE_DATE_TIME1 as CREATE_DATE_TIME1", \
	"RTR_INS_UPD_DEL_INS_UPD___MOD_DATE_TIME1 as MOD_DATE_TIME1", \
	"RTR_INS_UPD_DEL_INS_UPD___USER_ID1 as USER_ID1", \
	"RTR_INS_UPD_DEL_INS_UPD___MISC_TXT_11 as MISC_TXT_11", \
	"RTR_INS_UPD_DEL_INS_UPD___MISC_TXT_21 as MISC_TXT_21", \
	"RTR_INS_UPD_DEL_INS_UPD___MISC_NUM_11 as MISC_NUM_11", \
	"RTR_INS_UPD_DEL_INS_UPD___MISC_NUM_21 as MISC_NUM_21", \
	"RTR_INS_UPD_DEL_INS_UPD___VERSION_ID1 as VERSION_ID1", \
	"RTR_INS_UPD_DEL_INS_UPD___AVG_ACT_ID1 as AVG_ACT_ID1", \
	"RTR_INS_UPD_DEL_INS_UPD___AVG_BY1 as AVG_BY1", \
	"RTR_INS_UPD_DEL_INS_UPD___DELETE_FLAG1 as DELETE_FLAG1", \
	"RTR_INS_UPD_DEL_INS_UPD___UPDATE_TSTMP1 as UPDATE_TSTMP1", \
	"RTR_INS_UPD_DEL_INS_UPD___LOAD_TSTMP1 as LOAD_TSTMP1", \
	"RTR_INS_UPD_DEL_INS_UPD___LOAD_FLAG1 as LOAD_FLAG1") \
	.withColumn('pyspark_data_action', when(col('LOAD_FLAG1') ==(lit('INSERT')), lit(0)).when(col('LOAD_FLAG1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_ACT_ELM1, type TARGET 
# COLUMN COUNT: 19


# Shortcut_to_WM_E_ACT_ELM1 = UPD_DEL.selectExpr( \
# 	"CAST(in_LOCATION_ID3 AS BIGINT) as LOCATION_ID", \
# 	"CAST(in_WM_ACT_ID3 AS BIGINT) as WM_ACT_ID", \
# 	"CAST(in_WM_ELM_ID3 AS BIGINT) as WM_ELM_ID", \
# 	"CAST(NULL AS BIGINT) as WM_SEQ_NBR", \
# 	"CAST(NULL AS BIGINT) as TIME_ALLOW", \
# 	"CAST(NULL AS STRING) as WM_THRUPUT_MSRMNT", \
# 	"CAST(NULL AS BIGINT) as WM_AVG_ACT_ID", \
# 	"CAST(NULL AS STRING) as WM_AVG_BY", \
# 	"CAST(NULL AS STRING) as MISC_TXT_1", \
# 	"CAST(NULL AS STRING) as MISC_TXT_2", \
# 	"CAST(NULL AS BIGINT) as MISC_NUM_1", \
# 	"CAST(NULL AS BIGINT) as MISC_NUM_2", \
# 	"CAST(NULL AS STRING) as WM_USER_ID", \
# 	"CAST(NULL AS BIGINT) as WM_VERSION_ID", \
# 	"CAST(NULL AS TIMESTAMP) as WM_CREATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as WM_MOD_TSTMP", \
# 	"CAST(DELETE_FLAG3 AS BIGINT) as DELETE_FLAG", \
# 	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP" \
# )
# Shortcut_to_WM_E_ACT_ELM1.write.saveAsTable(f'{raw}.WM_E_ACT_ELM')

# COMMAND ----------
# Processing node Shortcut_to_WM_E_ACT_ELM2, type TARGET 
# COLUMN COUNT: 19

Shortcut_to_WM_E_ACT_ELM2 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(ACT_ID1 AS INT) as WM_ACT_ID",
	"CAST(ELM_ID1 AS INT) as WM_ELM_ID",
	"CAST(SEQ_NBR1 AS INT) as WM_SEQ_NBR",
	"CAST(TIME_ALLOW1 AS DECIMAL(9,4)) as TIME_ALLOW",
	"CAST(THRUPUT_MSRMNT1 AS STRING) as WM_THRUPUT_MSRMNT",
	"CAST(AVG_ACT_ID1 AS INT) as WM_AVG_ACT_ID",
	"CAST(AVG_BY1 AS STRING) as WM_AVG_BY",
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
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ACT_ID = target.WM_ACT_ID AND source.WM_ELM_ID = target.WM_ELM_ID"""
#   refined_perf_table = "WM_E_ACT_ELM"
  executeMerge(Shortcut_to_WM_E_ACT_ELM2, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_ACT_ELM", "WM_E_ACT_ELM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_ACT_ELM", "WM_E_ACT_ELM","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	