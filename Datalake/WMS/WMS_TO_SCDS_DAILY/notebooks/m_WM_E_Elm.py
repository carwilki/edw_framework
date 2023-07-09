#Code converted on 2023-06-26 09:59:11
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
raw_perf_table = f"{raw}.WM_E_ELM"
refined_perf_table = f"{refine}.WM_E_ELM_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table, refine,raw)
Del_Logic=args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_ELM, type SOURCE 
# COLUMN COUNT: 21

SQ_Shortcut_to_WM_E_ELM = spark.sql(f"""SELECT
LOCATION_ID,
WM_ELM_ID,
WM_ELM_NAME,
WM_ELM_DESC,
ORIG_NAME,
CORE_FLAG,
WM_MSRMNT_ID,
TIME_ALLOW,
WM_ELM_GRP_ID,
WM_UNQ_SEED_ID,
WM_SIM_WHSE,
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
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_ELM_PRE, type SOURCE 
# COLUMN COUNT: 19

SQ_Shortcut_to_WM_E_ELM_PRE = spark.sql(f"""SELECT
DC_NBR,
ELM_ID,
NAME,
DESCRIPTION,
CORE_FLAG,
MSRMNT_ID,
TIME_ALLOW,
ELM_GRP_ID,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
VERSION_ID,
UNQ_SEED_ID,
SIM_WHSE,
ORIG_NAME
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_ELM_PRE_temp = SQ_Shortcut_to_WM_E_ELM_PRE.toDF(*["SQ_Shortcut_to_WM_E_ELM_PRE___" + col for col in SQ_Shortcut_to_WM_E_ELM_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_ELM_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_ELM_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_ELM_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___ELM_ID as ELM_ID", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___NAME as NAME", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___CORE_FLAG as CORE_FLAG", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___MSRMNT_ID as MSRMNT_ID", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___TIME_ALLOW as TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___ELM_GRP_ID as ELM_GRP_ID", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___UNQ_SEED_ID as UNQ_SEED_ID", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___SIM_WHSE as SIM_WHSE", \
	"SQ_Shortcut_to_WM_E_ELM_PRE___ORIG_NAME as ORIG_NAME" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 21

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_ELM, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 40

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_ELM_temp = SQ_Shortcut_to_WM_E_ELM.toDF(*["SQ_Shortcut_to_WM_E_ELM___" + col for col in SQ_Shortcut_to_WM_E_ELM.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_E_ELM = SQ_Shortcut_to_WM_E_ELM_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_ELM_temp.SQ_Shortcut_to_WM_E_ELM___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_ELM_temp.SQ_Shortcut_to_WM_E_ELM___WM_ELM_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ELM_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ELM_ID as ELM_ID", \
	"JNR_SITE_PROFILE___NAME as NAME", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___CORE_FLAG as CORE_FLAG", \
	"JNR_SITE_PROFILE___MSRMNT_ID as MSRMNT_ID", \
	"JNR_SITE_PROFILE___TIME_ALLOW as TIME_ALLOW", \
	"JNR_SITE_PROFILE___ELM_GRP_ID as ELM_GRP_ID", \
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
	"JNR_SITE_PROFILE___ORIG_NAME as ORIG_NAME", \
	"SQ_Shortcut_to_WM_E_ELM___LOCATION_ID as i_LOCATION_ID1", \
	"SQ_Shortcut_to_WM_E_ELM___WM_ELM_ID as i_WM_ELM_ID", \
	"SQ_Shortcut_to_WM_E_ELM___WM_ELM_NAME as i_WM_ELM_NAME", \
	"SQ_Shortcut_to_WM_E_ELM___WM_ELM_DESC as i_WM_ELM_DESC", \
	"SQ_Shortcut_to_WM_E_ELM___ORIG_NAME as i_ORIG_NAME1", \
	"SQ_Shortcut_to_WM_E_ELM___CORE_FLAG as i_CORE_FLAG1", \
	"SQ_Shortcut_to_WM_E_ELM___WM_MSRMNT_ID as i_WM_MSRMNT_ID", \
	"SQ_Shortcut_to_WM_E_ELM___TIME_ALLOW as i_TIME_ALLOW1", \
	"SQ_Shortcut_to_WM_E_ELM___WM_ELM_GRP_ID as i_WM_ELM_GRP_ID", \
	"SQ_Shortcut_to_WM_E_ELM___WM_UNQ_SEED_ID as i_WM_UNQ_SEED_ID", \
	"SQ_Shortcut_to_WM_E_ELM___WM_SIM_WHSE as i_WM_SIM_WHSE", \
	"SQ_Shortcut_to_WM_E_ELM___MISC_TXT_1 as i_MISC_TXT_11", \
	"SQ_Shortcut_to_WM_E_ELM___MISC_TXT_2 as i_MISC_TXT_21", \
	"SQ_Shortcut_to_WM_E_ELM___MISC_NUM_1 as i_MISC_NUM_11", \
	"SQ_Shortcut_to_WM_E_ELM___MISC_NUM_2 as i_MISC_NUM_21", \
	"SQ_Shortcut_to_WM_E_ELM___WM_USER_ID as i_WM_USER_ID", \
	"SQ_Shortcut_to_WM_E_ELM___WM_VERSION_ID as i_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_ELM___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_ELM___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_E_ELM___DELETE_FLAG as i_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_E_ELM___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_ROWS, type FILTER 
# COLUMN COUNT: 40

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_ELM_temp = JNR_WM_E_ELM.toDF(*["JNR_WM_E_ELM___" + col for col in JNR_WM_E_ELM.columns])

FIL_UNCHANGED_ROWS = JNR_WM_E_ELM_temp.selectExpr( \
	"JNR_WM_E_ELM___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_ELM___ELM_ID as ELM_ID", \
	"JNR_WM_E_ELM___NAME as NAME", \
	"JNR_WM_E_ELM___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_E_ELM___CORE_FLAG as CORE_FLAG", \
	"JNR_WM_E_ELM___MSRMNT_ID as MSRMNT_ID", \
	"JNR_WM_E_ELM___TIME_ALLOW as TIME_ALLOW", \
	"JNR_WM_E_ELM___ELM_GRP_ID as ELM_GRP_ID", \
	"JNR_WM_E_ELM___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_ELM___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_ELM___USER_ID as USER_ID", \
	"JNR_WM_E_ELM___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_ELM___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_ELM___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_ELM___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_ELM___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_ELM___UNQ_SEED_ID as UNQ_SEED_ID", \
	"JNR_WM_E_ELM___SIM_WHSE as SIM_WHSE", \
	"JNR_WM_E_ELM___ORIG_NAME as ORIG_NAME", \
	"JNR_WM_E_ELM___i_LOCATION_ID1 as i_LOCATION_ID1", \
	"JNR_WM_E_ELM___i_WM_ELM_ID as i_WM_ELM_ID", \
	"JNR_WM_E_ELM___i_WM_ELM_NAME as i_WM_ELM_NAME", \
	"JNR_WM_E_ELM___i_WM_ELM_DESC as i_WM_ELM_DESC", \
	"JNR_WM_E_ELM___i_ORIG_NAME1 as i_ORIG_NAME1", \
	"JNR_WM_E_ELM___i_CORE_FLAG1 as i_CORE_FLAG1", \
	"JNR_WM_E_ELM___i_WM_MSRMNT_ID as i_WM_MSRMNT_ID", \
	"JNR_WM_E_ELM___i_TIME_ALLOW1 as i_TIME_ALLOW1", \
	"JNR_WM_E_ELM___i_WM_ELM_GRP_ID as i_WM_ELM_GRP_ID", \
	"JNR_WM_E_ELM___i_WM_UNQ_SEED_ID as i_WM_UNQ_SEED_ID", \
	"JNR_WM_E_ELM___i_WM_SIM_WHSE as i_WM_SIM_WHSE", \
	"JNR_WM_E_ELM___i_MISC_TXT_11 as i_MISC_TXT_11", \
	"JNR_WM_E_ELM___i_MISC_TXT_21 as i_MISC_TXT_21", \
	"JNR_WM_E_ELM___i_MISC_NUM_11 as i_MISC_NUM_11", \
	"JNR_WM_E_ELM___i_MISC_NUM_21 as i_MISC_NUM_21", \
	"JNR_WM_E_ELM___i_WM_USER_ID as i_WM_USER_ID", \
	"JNR_WM_E_ELM___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"JNR_WM_E_ELM___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_E_ELM___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_E_ELM___i_DELETE_FLAG as i_DELETE_FLAG", \
	"JNR_WM_E_ELM___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("ELM_ID is Null OR i_WM_ELM_ID is Null OR (  i_WM_ELM_ID is not Null AND \
             ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 44

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_ROWS_temp = FIL_UNCHANGED_ROWS.toDF(*["FIL_UNCHANGED_ROWS___" + col for col in FIL_UNCHANGED_ROWS.columns]) \
.withColumn("v_CREATE_DATE_TIME", expr("""IF(CREATE_DATE_TIME IS NULL, date'1900-01-01', CREATE_DATE_TIME)""")) \
	.withColumn("v_MOD_DATE_TIME", expr("""IF(MOD_DATE_TIME IS NULL, date'1900-01-01', MOD_DATE_TIME)""")) \
	.withColumn("v_i_WM_CREATE_TSTMP", expr("""IF(i_WM_CREATE_TSTMP IS NULL, date'1900-01-01', i_WM_CREATE_TSTMP)""")) \
	.withColumn("v_i_WM_MOD_TSTMP", expr("""IF(i_WM_MOD_TSTMP IS NULL, date'1900-01-01', i_WM_MOD_TSTMP)"""))
             
EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_ROWS_temp.selectExpr( \
	"FIL_UNCHANGED_ROWS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_ROWS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_ROWS___ELM_ID as ELM_ID", \
	"FIL_UNCHANGED_ROWS___NAME as NAME", \
	"FIL_UNCHANGED_ROWS___DESCRIPTION as DESCRIPTION", \
	"DECODE ( LTRIM ( RTRIM ( UPPER ( FIL_UNCHANGED_ROWS___CORE_FLAG ) ) ), 'Y','1', '1','1','0' ) as o_CORE_FLAG", \
	"FIL_UNCHANGED_ROWS___MSRMNT_ID as MSRMNT_ID", \
	"FIL_UNCHANGED_ROWS___TIME_ALLOW as TIME_ALLOW", \
	"FIL_UNCHANGED_ROWS___ELM_GRP_ID as ELM_GRP_ID", \
	"FIL_UNCHANGED_ROWS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_ROWS___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_ROWS___USER_ID as USER_ID", \
	"FIL_UNCHANGED_ROWS___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_UNCHANGED_ROWS___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_UNCHANGED_ROWS___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_UNCHANGED_ROWS___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_UNCHANGED_ROWS___VERSION_ID as VERSION_ID", \
	"FIL_UNCHANGED_ROWS___UNQ_SEED_ID as UNQ_SEED_ID", \
	"FIL_UNCHANGED_ROWS___SIM_WHSE as SIM_WHSE", \
	"FIL_UNCHANGED_ROWS___ORIG_NAME as ORIG_NAME", \
	"FIL_UNCHANGED_ROWS___i_LOCATION_ID1 as i_LOCATION_ID1", \
	"FIL_UNCHANGED_ROWS___i_WM_ELM_ID as i_WM_ELM_ID", \
	"FIL_UNCHANGED_ROWS___i_WM_ELM_NAME as i_WM_ELM_NAME", \
	"FIL_UNCHANGED_ROWS___i_WM_ELM_DESC as i_WM_ELM_DESC", \
	"FIL_UNCHANGED_ROWS___i_ORIG_NAME1 as i_ORIG_NAME1", \
	"FIL_UNCHANGED_ROWS___i_CORE_FLAG1 as i_CORE_FLAG1", \
	"FIL_UNCHANGED_ROWS___i_WM_MSRMNT_ID as i_WM_MSRMNT_ID", \
	"FIL_UNCHANGED_ROWS___i_TIME_ALLOW1 as i_TIME_ALLOW1", \
	"FIL_UNCHANGED_ROWS___i_WM_ELM_GRP_ID as i_WM_ELM_GRP_ID", \
	"FIL_UNCHANGED_ROWS___i_WM_UNQ_SEED_ID as i_WM_UNQ_SEED_ID", \
	"FIL_UNCHANGED_ROWS___i_WM_SIM_WHSE as i_WM_SIM_WHSE", \
	"FIL_UNCHANGED_ROWS___i_MISC_TXT_11 as i_MISC_TXT_11", \
	"FIL_UNCHANGED_ROWS___i_MISC_TXT_21 as i_MISC_TXT_21", \
	"FIL_UNCHANGED_ROWS___i_MISC_NUM_11 as i_MISC_NUM_11", \
	"FIL_UNCHANGED_ROWS___i_MISC_NUM_21 as i_MISC_NUM_21", \
	"FIL_UNCHANGED_ROWS___i_WM_USER_ID as i_WM_USER_ID", \
	"FIL_UNCHANGED_ROWS___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"FIL_UNCHANGED_ROWS___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"FIL_UNCHANGED_ROWS___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"FIL_UNCHANGED_ROWS___i_DELETE_FLAG as i_DELETE_FLAG", \
	"FIL_UNCHANGED_ROWS___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_ROWS___ELM_ID IS NULL AND FIL_UNCHANGED_ROWS___i_WM_ELM_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_ROWS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_ROWS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_ROWS___ELM_ID IS NOT NULL AND FIL_UNCHANGED_ROWS___i_WM_ELM_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_ROWS___ELM_ID IS NULL AND FIL_UNCHANGED_ROWS___i_WM_ELM_ID IS NOT NULL AND ( FIL_UNCHANGED_ROWS___v_i_WM_CREATE_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_ROWS___v_i_WM_MOD_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) ), 'DELETE', IF(FIL_UNCHANGED_ROWS___ELM_ID IS NOT NULL AND FIL_UNCHANGED_ROWS___i_WM_ELM_ID IS NOT NULL AND ( FIL_UNCHANGED_ROWS___v_i_WM_CREATE_TSTMP <> FIL_UNCHANGED_ROWS___v_CREATE_DATE_TIME OR FIL_UNCHANGED_ROWS___v_i_WM_MOD_TSTMP <> FIL_UNCHANGED_ROWS___v_MOD_DATE_TIME ), 'UPDATE', NULL))) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node RTR_INS_UPD_DEL, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 44


# Creating output dataframe for RTR_INS_UPD_DEL, output group DELETE
RTR_INS_UPD_DEL_DELETE = EXP_OUTPUT_VALIDATOR.selectExpr( \
	"EXP_OUTPUT_VALIDATOR.LOCATION_ID as LOCATION_ID", \
	"EXP_OUTPUT_VALIDATOR.ELM_ID as ELM_ID", \
	"EXP_OUTPUT_VALIDATOR.NAME as NAME", \
	"EXP_OUTPUT_VALIDATOR.DESCRIPTION as DESCRIPTION", \
	"EXP_OUTPUT_VALIDATOR.o_CORE_FLAG as CORE_FLAG", \
	"EXP_OUTPUT_VALIDATOR.MSRMNT_ID as MSRMNT_ID", \
	"EXP_OUTPUT_VALIDATOR.TIME_ALLOW as TIME_ALLOW", \
	"EXP_OUTPUT_VALIDATOR.ELM_GRP_ID as ELM_GRP_ID", \
	"EXP_OUTPUT_VALIDATOR.CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_OUTPUT_VALIDATOR.MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_OUTPUT_VALIDATOR.USER_ID as USER_ID", \
	"EXP_OUTPUT_VALIDATOR.MISC_TXT_1 as MISC_TXT_1", \
	"EXP_OUTPUT_VALIDATOR.MISC_TXT_2 as MISC_TXT_2", \
	"EXP_OUTPUT_VALIDATOR.MISC_NUM_1 as MISC_NUM_1", \
	"EXP_OUTPUT_VALIDATOR.MISC_NUM_2 as MISC_NUM_2", \
	"EXP_OUTPUT_VALIDATOR.VERSION_ID as VERSION_ID", \
	"EXP_OUTPUT_VALIDATOR.UNQ_SEED_ID as UNQ_SEED_ID", \
	"EXP_OUTPUT_VALIDATOR.SIM_WHSE as SIM_WHSE", \
	"EXP_OUTPUT_VALIDATOR.ORIG_NAME as ORIG_NAME", \
	"EXP_OUTPUT_VALIDATOR.i_LOCATION_ID1 as i_LOCATION_ID1", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_ID as i_WM_ELM_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_NAME as i_WM_ELM_NAME", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_DESC as i_WM_ELM_DESC", \
	"EXP_OUTPUT_VALIDATOR.i_ORIG_NAME1 as i_ORIG_NAME1", \
	"EXP_OUTPUT_VALIDATOR.i_CORE_FLAG1 as i_CORE_FLAG1", \
	"EXP_OUTPUT_VALIDATOR.i_WM_MSRMNT_ID as i_WM_MSRMNT_ID", \
	"EXP_OUTPUT_VALIDATOR.i_TIME_ALLOW1 as i_TIME_ALLOW1", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_GRP_ID as i_WM_ELM_GRP_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_UNQ_SEED_ID as i_WM_UNQ_SEED_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_SIM_WHSE as i_WM_SIM_WHSE", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_TXT_11 as i_MISC_TXT_11", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_TXT_21 as i_MISC_TXT_21", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_NUM_11 as i_MISC_NUM_11", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_NUM_21 as i_MISC_NUM_21", \
	"EXP_OUTPUT_VALIDATOR.i_WM_USER_ID as i_WM_USER_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.i_DELETE_FLAG as i_DELETE_FLAG", \
	"EXP_OUTPUT_VALIDATOR.i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.DELETE_FLAG as DELETE_FLAG", \
	"EXP_OUTPUT_VALIDATOR.UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR").select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID3'), \
	col('ELM_ID').alias('ELM_ID3'), \
	col('NAME').alias('NAME3'), \
	col('DESCRIPTION').alias('DESCRIPTION3'), \
	col('CORE_FLAG').alias('CORE_FLAG3'), \
	col('MSRMNT_ID').alias('MSRMNT_ID3'), \
	col('TIME_ALLOW').alias('TIME_ALLOW3'), \
	col('ELM_GRP_ID').alias('ELM_GRP_ID3'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME3'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME3'), \
	col('USER_ID').alias('USER_ID3'), \
	col('MISC_TXT_1').alias('MISC_TXT_13'), \
	col('MISC_TXT_2').alias('MISC_TXT_23'), \
	col('MISC_NUM_1').alias('MISC_NUM_13'), \
	col('MISC_NUM_2').alias('MISC_NUM_23'), \
	col('VERSION_ID').alias('VERSION_ID3'), \
	col('UNQ_SEED_ID').alias('UNQ_SEED_ID3'), \
	col('SIM_WHSE').alias('SIM_WHSE3'), \
	col('ORIG_NAME').alias('ORIG_NAME3'), \
	col('i_LOCATION_ID1').alias('i_LOCATION_ID13'), \
	col('i_WM_ELM_ID').alias('i_WM_ELM_ID3'), \
	col('i_WM_ELM_NAME').alias('i_WM_ELM_NAME3'), \
	col('i_WM_ELM_DESC').alias('i_WM_ELM_DESC3'), \
	col('i_ORIG_NAME1').alias('i_ORIG_NAME13'), \
	col('i_CORE_FLAG1').alias('i_CORE_FLAG13'), \
	col('i_WM_MSRMNT_ID').alias('i_WM_MSRMNT_ID3'), \
	col('i_TIME_ALLOW1').alias('i_TIME_ALLOW13'), \
	col('i_WM_ELM_GRP_ID').alias('i_WM_ELM_GRP_ID3'), \
	col('i_WM_UNQ_SEED_ID').alias('i_WM_UNQ_SEED_ID3'), \
	col('i_WM_SIM_WHSE').alias('i_WM_SIM_WHSE3'), \
	col('i_MISC_TXT_11').alias('i_MISC_TXT_113'), \
	col('i_MISC_TXT_21').alias('i_MISC_TXT_213'), \
	col('i_MISC_NUM_11').alias('i_MISC_NUM_113'), \
	col('i_MISC_NUM_21').alias('i_MISC_NUM_213'), \
	col('i_WM_USER_ID').alias('i_WM_USER_ID3'), \
	col('i_WM_VERSION_ID').alias('i_WM_VERSION_ID3'), \
	col('i_WM_CREATE_TSTMP').alias('i_WM_CREATE_TSTMP3'), \
	col('i_WM_MOD_TSTMP').alias('i_WM_MOD_TSTMP3'), \
	col('i_DELETE_FLAG').alias('i_DELETE_FLAG3'), \
	col('i_LOAD_TSTMP').alias('i_LOAD_TSTMP3'), \
	col('DELETE_FLAG').alias('DELETE_FLAG3'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP3'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP3'), \
	col('o_UPDATE_VALIDATOR').alias('o_UPDATE_VALIDATOR3')).filter("o_UPDATE_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_INS_UPD_DEL, output group INSERT_UPDATE
RTR_INS_UPD_DEL_INSERT_UPDATE = EXP_OUTPUT_VALIDATOR.selectExpr( \
	"EXP_OUTPUT_VALIDATOR.LOCATION_ID as LOCATION_ID", \
	"EXP_OUTPUT_VALIDATOR.ELM_ID as ELM_ID", \
	"EXP_OUTPUT_VALIDATOR.NAME as NAME", \
	"EXP_OUTPUT_VALIDATOR.DESCRIPTION as DESCRIPTION", \
	"EXP_OUTPUT_VALIDATOR.o_CORE_FLAG as CORE_FLAG", \
	"EXP_OUTPUT_VALIDATOR.MSRMNT_ID as MSRMNT_ID", \
	"EXP_OUTPUT_VALIDATOR.TIME_ALLOW as TIME_ALLOW", \
	"EXP_OUTPUT_VALIDATOR.ELM_GRP_ID as ELM_GRP_ID", \
	"EXP_OUTPUT_VALIDATOR.CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_OUTPUT_VALIDATOR.MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_OUTPUT_VALIDATOR.USER_ID as USER_ID", \
	"EXP_OUTPUT_VALIDATOR.MISC_TXT_1 as MISC_TXT_1", \
	"EXP_OUTPUT_VALIDATOR.MISC_TXT_2 as MISC_TXT_2", \
	"EXP_OUTPUT_VALIDATOR.MISC_NUM_1 as MISC_NUM_1", \
	"EXP_OUTPUT_VALIDATOR.MISC_NUM_2 as MISC_NUM_2", \
	"EXP_OUTPUT_VALIDATOR.VERSION_ID as VERSION_ID", \
	"EXP_OUTPUT_VALIDATOR.UNQ_SEED_ID as UNQ_SEED_ID", \
	"EXP_OUTPUT_VALIDATOR.SIM_WHSE as SIM_WHSE", \
	"EXP_OUTPUT_VALIDATOR.ORIG_NAME as ORIG_NAME", \
	"EXP_OUTPUT_VALIDATOR.i_LOCATION_ID1 as i_LOCATION_ID1", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_ID as i_WM_ELM_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_NAME as i_WM_ELM_NAME", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_DESC as i_WM_ELM_DESC", \
	"EXP_OUTPUT_VALIDATOR.i_ORIG_NAME1 as i_ORIG_NAME1", \
	"EXP_OUTPUT_VALIDATOR.i_CORE_FLAG1 as i_CORE_FLAG1", \
	"EXP_OUTPUT_VALIDATOR.i_WM_MSRMNT_ID as i_WM_MSRMNT_ID", \
	"EXP_OUTPUT_VALIDATOR.i_TIME_ALLOW1 as i_TIME_ALLOW1", \
	"EXP_OUTPUT_VALIDATOR.i_WM_ELM_GRP_ID as i_WM_ELM_GRP_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_UNQ_SEED_ID as i_WM_UNQ_SEED_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_SIM_WHSE as i_WM_SIM_WHSE", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_TXT_11 as i_MISC_TXT_11", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_TXT_21 as i_MISC_TXT_21", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_NUM_11 as i_MISC_NUM_11", \
	"EXP_OUTPUT_VALIDATOR.i_MISC_NUM_21 as i_MISC_NUM_21", \
	"EXP_OUTPUT_VALIDATOR.i_WM_USER_ID as i_WM_USER_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"EXP_OUTPUT_VALIDATOR.i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.i_DELETE_FLAG as i_DELETE_FLAG", \
	"EXP_OUTPUT_VALIDATOR.i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.DELETE_FLAG as DELETE_FLAG", \
	"EXP_OUTPUT_VALIDATOR.UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR").select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID1'), \
	col('ELM_ID').alias('ELM_ID1'), \
	col('NAME').alias('NAME1'), \
	col('DESCRIPTION').alias('DESCRIPTION1'), \
	col('CORE_FLAG').alias('CORE_FLAG1'), \
	col('MSRMNT_ID').alias('MSRMNT_ID1'), \
	col('TIME_ALLOW').alias('TIME_ALLOW1'), \
	col('ELM_GRP_ID').alias('ELM_GRP_ID1'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME1'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME1'), \
	col('USER_ID').alias('USER_ID1'), \
	col('MISC_TXT_1').alias('MISC_TXT_11'), \
	col('MISC_TXT_2').alias('MISC_TXT_21'), \
	col('MISC_NUM_1').alias('MISC_NUM_11'), \
	col('MISC_NUM_2').alias('MISC_NUM_21'), \
	col('VERSION_ID').alias('VERSION_ID1'), \
	col('UNQ_SEED_ID').alias('UNQ_SEED_ID1'), \
	col('SIM_WHSE').alias('SIM_WHSE1'), \
	col('ORIG_NAME').alias('ORIG_NAME1'), \
	col('i_LOCATION_ID1').alias('i_LOCATION_ID11'), \
	col('i_WM_ELM_ID').alias('i_WM_ELM_ID1'), \
	col('i_WM_ELM_NAME').alias('i_WM_ELM_NAME1'), \
	col('i_WM_ELM_DESC').alias('i_WM_ELM_DESC1'), \
	col('i_ORIG_NAME1').alias('i_ORIG_NAME11'), \
	col('i_CORE_FLAG1').alias('i_CORE_FLAG11'), \
	col('i_WM_MSRMNT_ID').alias('i_WM_MSRMNT_ID1'), \
	col('i_TIME_ALLOW1').alias('i_TIME_ALLOW11'), \
	col('i_WM_ELM_GRP_ID').alias('i_WM_ELM_GRP_ID1'), \
	col('i_WM_UNQ_SEED_ID').alias('i_WM_UNQ_SEED_ID1'), \
	col('i_WM_SIM_WHSE').alias('i_WM_SIM_WHSE1'), \
	col('i_MISC_TXT_11').alias('i_MISC_TXT_111'), \
	col('i_MISC_TXT_21').alias('i_MISC_TXT_211'), \
	col('i_MISC_NUM_11').alias('i_MISC_NUM_111'), \
	col('i_MISC_NUM_21').alias('i_MISC_NUM_211'), \
	col('i_WM_USER_ID').alias('i_WM_USER_ID1'), \
	col('i_WM_VERSION_ID').alias('i_WM_VERSION_ID1'), \
	col('i_WM_CREATE_TSTMP').alias('i_WM_CREATE_TSTMP1'), \
	col('i_WM_MOD_TSTMP').alias('i_WM_MOD_TSTMP1'), \
	col('i_DELETE_FLAG').alias('i_DELETE_FLAG1'), \
	col('i_LOAD_TSTMP').alias('i_LOAD_TSTMP1'), \
	col('DELETE_FLAG').alias('DELETE_FLAG1'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP1'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP1'), \
	col('o_UPDATE_VALIDATOR').alias('o_UPDATE_VALIDATOR1')).filter("o_UPDATE_VALIDATOR = 'INSERT' OR o_UPDATE_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_INSERT_UPDATE_temp = RTR_INS_UPD_DEL_INSERT_UPDATE.toDF(*["RTR_INS_UPD_DEL_INSERT_UPDATE___" + col for col in RTR_INS_UPD_DEL_INSERT_UPDATE.columns])

UPD_INS_UPD = RTR_INS_UPD_DEL_INSERT_UPDATE_temp.selectExpr( \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___ELM_ID1 as ELM_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___NAME1 as NAME1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___DESCRIPTION1 as DESCRIPTION1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CORE_FLAG1 as CORE_FLAG1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MSRMNT_ID1 as MSRMNT_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___TIME_ALLOW1 as TIME_ALLOW1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___ELM_GRP_ID1 as ELM_GRP_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATE_DATE_TIME1 as CREATE_DATE_TIME1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MOD_DATE_TIME1 as MOD_DATE_TIME1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___USER_ID1 as USER_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_TXT_11 as MISC_TXT_11", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_TXT_21 as MISC_TXT_21", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_NUM_11 as MISC_NUM_11", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_NUM_21 as MISC_NUM_21", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___VERSION_ID1 as VERSION_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___UNQ_SEED_ID1 as UNQ_SEED_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___SIM_WHSE1 as SIM_WHSE1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___ORIG_NAME1 as ORIG_NAME1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___DELETE_FLAG1 as DELETE_FLAG1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___o_UPDATE_VALIDATOR1 as o_UPDATE_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(RTR_INS_UPD_DEL_INSERT_UPDATE.o_UPDATE_VALIDATOR1 ==(lit('INSERT')), lit(0)).when(RTR_INS_UPD_DEL_INSERT_UPDATE.o_UPDATE_VALIDATOR1 ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_DELETE_temp = RTR_INS_UPD_DEL_DELETE.toDF(*["RTR_INS_UPD_DEL_DELETE___" + col for col in RTR_INS_UPD_DEL_DELETE.columns])

UPD_DELETE = RTR_INS_UPD_DEL_DELETE_temp.selectExpr( \
	"RTR_INS_UPD_DEL_DELETE___i_LOCATION_ID13 as i_LOCATION_ID13", \
	"RTR_INS_UPD_DEL_DELETE___i_WM_ELM_ID3 as i_WM_ELM_ID3", \
	"RTR_INS_UPD_DEL_DELETE___DELETE_FLAG3 as DELETE_FLAG3", \
	"RTR_INS_UPD_DEL_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_ELM1, type TARGET 
# COLUMN COUNT: 22

Shortcut_to_WM_E_ELM11 = UPD_DELETE.selectExpr( 
	"CAST(i_LOCATION_ID13 AS BIGINT) as LOCATION_ID", 
	"CAST(i_WM_ELM_ID3 AS BIGINT) as WM_ELM_ID", 
	"CAST(NULL AS STRING) as WM_ELM_NAME", 
	"CAST(NULL AS STRING) as WM_ELM_DESC", 
	"CAST(NULL AS STRING) as ORIG_NAME", 
	"CAST(NULL AS BIGINT) as CORE_FLAG", 
	"CAST(NULL AS BIGINT) as WM_MSRMNT_ID", 
	"CAST(NULL AS BIGINT) as TIME_ALLOW", 
	"CAST(NULL AS BIGINT) as WM_ELM_GRP_ID", 
	"CAST(NULL AS BIGINT) as WM_UNQ_SEED_ID", 
	"CAST(NULL AS STRING) as WM_SIM_WHSE", 
	"CAST(NULL AS STRING) as MISC_TXT_1", 
	"CAST(NULL AS STRING) as MISC_TXT_2", 
	"CAST(NULL AS BIGINT) as MISC_NUM_1", 
	"CAST(NULL AS BIGINT) as MISC_NUM_2", 
	"CAST(NULL AS STRING) as WM_USER_ID", 
	"CAST(NULL AS BIGINT) as WM_VERSION_ID", 
	"CAST(NULL AS TIMESTAMP) as WM_CREATE_TSTMP", 
	"CAST(NULL AS TIMESTAMP) as WM_MOD_TSTMP", 
	"CAST(DELETE_FLAG3 AS BIGINT) as DELETE_FLAG", 
	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ELM_ID = target.WM_ELM_ID"""
#   refined_perf_table = "WM_E_ELM"
  executeMerge(Shortcut_to_WM_E_ELM1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_ELM", "WM_E_ELM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_ELM", "WM_E_ELM","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------
# Processing node Shortcut_to_WM_E_ELM11, type TARGET 
# COLUMN COUNT: 22


# Shortcut_to_WM_E_ELM11 = UPD_DELETE.selectExpr( \
# 	"CAST(i_LOCATION_ID13 AS BIGINT) as LOCATION_ID", \
# 	"CAST(i_WM_ELM_ID3 AS BIGINT) as WM_ELM_ID", \
# 	"CAST(NULL AS STRING) as WM_ELM_NAME", \
# 	"CAST(NULL AS STRING) as WM_ELM_DESC", \
# 	"CAST(NULL AS STRING) as ORIG_NAME", \
# 	"CAST(NULL AS BIGINT) as CORE_FLAG", \
# 	"CAST(NULL AS BIGINT) as WM_MSRMNT_ID", \
# 	"CAST(NULL AS BIGINT) as TIME_ALLOW", \
# 	"CAST(NULL AS BIGINT) as WM_ELM_GRP_ID", \
# 	"CAST(NULL AS BIGINT) as WM_UNQ_SEED_ID", \
# 	"CAST(NULL AS STRING) as WM_SIM_WHSE", \
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
# Shortcut_to_WM_E_ELM11.write.saveAsTable(f'{raw}.WM_E_ELM')