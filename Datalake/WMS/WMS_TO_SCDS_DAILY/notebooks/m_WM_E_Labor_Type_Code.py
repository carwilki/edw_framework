#Code converted on 2023-06-26 09:56:31
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
raw_perf_table = f"{raw}.WM_E_LABOR_TYPE_CODE_PRE"
refined_perf_table = f"{refine}.WM_E_LABOR_TYPE_CODE"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic= ' -- '  #args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE = spark.sql(f"""SELECT
DC_NBR,
LABOR_TYPE_ID,
LABOR_TYPE_CODE,
DESCRIPTION,
USER_ID,
CREATE_DATE_TIME,
MOD_DATE_TIME,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
VERSION_ID,
SPVSR_AUTH_REQUIRED,
CREATED_DTTM,
LAST_UPDATED_DTTM
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE = spark.sql(f"""SELECT
LOCATION_ID,
WM_LABOR_TYPE_ID,
WM_LABOR_TYPE_CD,
WM_LABOR_TYPE_DESC,
SPVSR_AUTH_REQUIRED_FLAG,
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
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE_temp = SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE.toDF(*["SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___" + col for col in SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___LABOR_TYPE_ID as LABOR_TYPE_ID", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___LABOR_TYPE_CODE as LABOR_TYPE_CODE", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___SPVSR_AUTH_REQUIRED as SPVSR_AUTH_REQUIRED", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 17

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_E_LABOR_TYPE_CODE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_temp = SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE.toDF(*["SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___" + col for col in SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_E_LABOR_TYPE_CODE = SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_temp.SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE_temp.SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_LABOR_TYPE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LABOR_TYPE_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___LABOR_TYPE_ID as LABOR_TYPE_ID", \
	"JNR_SITE_PROFILE___LABOR_TYPE_CODE as LABOR_TYPE_CODE", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___SPVSR_AUTH_REQUIRED as SPVSR_AUTH_REQUIRED", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___LOCATION_ID as i_LOCATION_ID1", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_LABOR_TYPE_ID as i_WM_LABOR_TYPE_ID", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_LABOR_TYPE_CD as i_WM_LABOR_TYPE_CD", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_LABOR_TYPE_DESC as i_WM_LABOR_TYPE_DESC", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___SPVSR_AUTH_REQUIRED_FLAG as i_SPVSR_AUTH_REQUIRED_FLAG", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___MISC_TXT_1 as i_MISC_TXT_11", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___MISC_TXT_2 as i_MISC_TXT_21", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___MISC_NUM_1 as i_MISC_NUM_11", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___MISC_NUM_2 as i_MISC_NUM_21", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_USER_ID as i_WM_USER_ID", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_VERSION_ID as i_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___DELETE_FLAG as i_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_E_LABOR_TYPE_CODE___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_E_LABOR_TYPE_CODE_temp = JNR_WM_E_LABOR_TYPE_CODE.toDF(*["JNR_WM_E_LABOR_TYPE_CODE___" + col for col in JNR_WM_E_LABOR_TYPE_CODE.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_E_LABOR_TYPE_CODE_temp.selectExpr( \
	"JNR_WM_E_LABOR_TYPE_CODE___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___LABOR_TYPE_ID as LABOR_TYPE_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___LABOR_TYPE_CODE as LABOR_TYPE_CODE", \
	"JNR_WM_E_LABOR_TYPE_CODE___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_E_LABOR_TYPE_CODE___USER_ID as USER_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_E_LABOR_TYPE_CODE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_E_LABOR_TYPE_CODE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_WM_E_LABOR_TYPE_CODE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_WM_E_LABOR_TYPE_CODE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_WM_E_LABOR_TYPE_CODE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_WM_E_LABOR_TYPE_CODE___VERSION_ID as VERSION_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___SPVSR_AUTH_REQUIRED as SPVSR_AUTH_REQUIRED", \
	"JNR_WM_E_LABOR_TYPE_CODE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_E_LABOR_TYPE_CODE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_LOCATION_ID1 as i_LOCATION_ID1", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_LABOR_TYPE_ID as i_WM_LABOR_TYPE_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_LABOR_TYPE_CD as i_WM_LABOR_TYPE_CD", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_LABOR_TYPE_DESC as i_WM_LABOR_TYPE_DESC", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_SPVSR_AUTH_REQUIRED_FLAG as i_SPVSR_AUTH_REQUIRED_FLAG", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_MISC_TXT_11 as i_MISC_TXT_11", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_MISC_TXT_21 as i_MISC_TXT_21", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_MISC_NUM_11 as i_MISC_NUM_11", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_MISC_NUM_21 as i_MISC_NUM_21", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_USER_ID as i_WM_USER_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_DELETE_FLAG as i_DELETE_FLAG", \
	"JNR_WM_E_LABOR_TYPE_CODE___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("LABOR_TYPE_ID IS NULL OR i_WM_LABOR_TYPE_ID IS NULL OR (  i_WM_LABOR_TYPE_ID IS NOT NULL AND \
             ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01') \
             OR COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
.withColumn("FIL_UNCHANGED_RECORDS___v_CREATE_DATE_TIME", expr("""IF(FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_MOD_DATE_TIME", expr("""IF( FIL_UNCHANGED_RECORDS___MOD_DATE_TIME IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___MOD_DATE_TIME)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_CREATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___CREATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___CREATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_CREATE_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_CREATE_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_CREATE_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_MOD_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_MOD_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_MOD_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP)"""))
             
EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___LABOR_TYPE_ID as LABOR_TYPE_ID", \
	"FIL_UNCHANGED_RECORDS___LABOR_TYPE_CODE as LABOR_TYPE_CODE", \
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", \
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_UNCHANGED_RECORDS___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_UNCHANGED_RECORDS___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_UNCHANGED_RECORDS___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_UNCHANGED_RECORDS___VERSION_ID as VERSION_ID", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___SPVSR_AUTH_REQUIRED)) IN ('Y', '1') THEN '1' ELSE '0' END as o_SPVSR_AUTH_REQUIRED", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 as i_LOCATION_ID1", \
	"FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_ID as i_WM_LABOR_TYPE_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_CD as i_WM_LABOR_TYPE_CD", \
	"FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_DESC as i_WM_LABOR_TYPE_DESC", \
	"FIL_UNCHANGED_RECORDS___i_SPVSR_AUTH_REQUIRED_FLAG as i_SPVSR_AUTH_REQUIRED_FLAG", \
	"FIL_UNCHANGED_RECORDS___i_MISC_TXT_11 as i_MISC_TXT_11", \
	"FIL_UNCHANGED_RECORDS___i_MISC_TXT_21 as i_MISC_TXT_21", \
	"FIL_UNCHANGED_RECORDS___i_MISC_NUM_11 as i_MISC_NUM_11", \
	"FIL_UNCHANGED_RECORDS___i_MISC_NUM_21 as i_MISC_NUM_21", \
	"FIL_UNCHANGED_RECORDS___i_WM_USER_ID as i_WM_USER_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_DELETE_FLAG as i_DELETE_FLAG", \
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___LABOR_TYPE_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	f"IF(FIL_UNCHANGED_RECORDS___LABOR_TYPE_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_RECORDS___LABOR_TYPE_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATE_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_UNCHANGED_RECORDS___v_i_WM_MOD_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) ), 'DELETE', IF(FIL_UNCHANGED_RECORDS___LABOR_TYPE_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_LABOR_TYPE_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATE_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATE_DATE_TIME OR FIL_UNCHANGED_RECORDS___v_i_WM_MOD_TSTMP <> FIL_UNCHANGED_RECORDS___v_MOD_DATE_TIME OR FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM ), 'UPDATE', NULL))) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node RTR_INS_UPD_DEL, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36


# Creating output dataframe for RTR_INS_UPD_DEL, output group DELETE
RTR_INS_UPD_DEL_DELETE = EXP_OUTPUT_VALIDATOR.selectExpr( \
	"LOCATION_ID as LOCATION_ID", \
	"LABOR_TYPE_ID as LABOR_TYPE_ID", \
	"LABOR_TYPE_CODE as LABOR_TYPE_CODE", \
	"DESCRIPTION as DESCRIPTION", \
	"USER_ID as USER_ID", \
	"CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"MOD_DATE_TIME as MOD_DATE_TIME", \
	"MISC_TXT_1 as MISC_TXT_1", \
	"MISC_TXT_2 as MISC_TXT_2", \
	"MISC_NUM_1 as MISC_NUM_1", \
	"MISC_NUM_2 as MISC_NUM_2", \
	"VERSION_ID as VERSION_ID", \
	"o_SPVSR_AUTH_REQUIRED as SPVSR_AUTH_REQUIRED", \
	"CREATED_DTTM as CREATED_DTTM", \
	"LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"i_LOCATION_ID1 as i_LOCATION_ID1", \
	"i_WM_LABOR_TYPE_ID as i_WM_LABOR_TYPE_ID", \
	"i_WM_LABOR_TYPE_CD as i_WM_LABOR_TYPE_CD", \
	"i_WM_LABOR_TYPE_DESC as i_WM_LABOR_TYPE_DESC", \
	"i_SPVSR_AUTH_REQUIRED_FLAG as i_SPVSR_AUTH_REQUIRED_FLAG", \
	"i_MISC_TXT_11 as i_MISC_TXT_11", \
	"i_MISC_TXT_21 as i_MISC_TXT_21", \
	"i_MISC_NUM_11 as i_MISC_NUM_11", \
	"i_MISC_NUM_21 as i_MISC_NUM_21", \
	"i_WM_USER_ID as i_WM_USER_ID", \
	"i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"i_DELETE_FLAG as i_DELETE_FLAG", \
	"i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"DELETE_FLAG as DELETE_FLAG", \
	"UPDATE_TSTMP as UPDATE_TSTMP", \
	"LOAD_TSTMP as LOAD_TSTMP", \
	"o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR" ,\
	"sys_row_id as sys_row_id").select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID3'), \
	col('LABOR_TYPE_ID').alias('LABOR_TYPE_ID3'), \
	col('LABOR_TYPE_CODE').alias('LABOR_TYPE_CODE3'), \
	col('DESCRIPTION').alias('DESCRIPTION3'), \
	col('USER_ID').alias('USER_ID3'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME3'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME3'), \
	col('MISC_TXT_1').alias('MISC_TXT_13'), \
	col('MISC_TXT_2').alias('MISC_TXT_23'), \
	col('MISC_NUM_1').alias('MISC_NUM_13'), \
	col('MISC_NUM_2').alias('MISC_NUM_23'), \
	col('VERSION_ID').alias('VERSION_ID3'), \
	col('SPVSR_AUTH_REQUIRED').alias('SPVSR_AUTH_REQUIRED3'), \
	col('CREATED_DTTM').alias('CREATED_DTTM3'), \
	col('LAST_UPDATED_DTTM').alias('LAST_UPDATED_DTTM3'), \
	col('i_LOCATION_ID1').alias('i_LOCATION_ID13'), \
	col('i_WM_LABOR_TYPE_ID').alias('i_WM_LABOR_TYPE_ID3'), \
	col('i_WM_LABOR_TYPE_CD').alias('i_WM_LABOR_TYPE_CD3'), \
	col('i_WM_LABOR_TYPE_DESC').alias('i_WM_LABOR_TYPE_DESC3'), \
	col('i_SPVSR_AUTH_REQUIRED_FLAG').alias('i_SPVSR_AUTH_REQUIRED_FLAG3'), \
	col('i_MISC_TXT_11').alias('i_MISC_TXT_113'), \
	col('i_MISC_TXT_21').alias('i_MISC_TXT_213'), \
	col('i_MISC_NUM_11').alias('i_MISC_NUM_113'), \
	col('i_MISC_NUM_21').alias('i_MISC_NUM_213'), \
	col('i_WM_USER_ID').alias('i_WM_USER_ID3'), \
	col('i_WM_VERSION_ID').alias('i_WM_VERSION_ID3'), \
	col('i_WM_CREATE_TSTMP').alias('i_WM_CREATE_TSTMP3'), \
	col('i_WM_MOD_TSTMP').alias('i_WM_MOD_TSTMP3'), \
	col('i_WM_CREATED_TSTMP').alias('i_WM_CREATED_TSTMP3'), \
	col('i_WM_LAST_UPDATED_TSTMP').alias('i_WM_LAST_UPDATED_TSTMP3'), \
	col('i_DELETE_FLAG').alias('i_DELETE_FLAG3'), \
	col('i_LOAD_TSTMP').alias('i_LOAD_TSTMP3'), \
	col('DELETE_FLAG').alias('DELETE_FLAG3'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP3'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP3'), \
	col('o_UPDATE_VALIDATOR').alias('o_UPDATE_VALIDATOR3')).filter("o_UPDATE_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_INS_UPD_DEL, output group INSERT_UPDATE
RTR_INS_UPD_DEL_INSERT_UPDATE = EXP_OUTPUT_VALIDATOR.selectExpr( \
	"LOCATION_ID as LOCATION_ID", \
	"LABOR_TYPE_ID as LABOR_TYPE_ID", \
	"LABOR_TYPE_CODE as LABOR_TYPE_CODE", \
	"DESCRIPTION as DESCRIPTION", \
	"USER_ID as USER_ID", \
	"CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"MOD_DATE_TIME as MOD_DATE_TIME", \
	"MISC_TXT_1 as MISC_TXT_1", \
	"MISC_TXT_2 as MISC_TXT_2", \
	"MISC_NUM_1 as MISC_NUM_1", \
	"MISC_NUM_2 as MISC_NUM_2", \
	"VERSION_ID as VERSION_ID", \
	"o_SPVSR_AUTH_REQUIRED as SPVSR_AUTH_REQUIRED", \
	"CREATED_DTTM as CREATED_DTTM", \
	"LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"i_LOCATION_ID1 as i_LOCATION_ID1", \
	"i_WM_LABOR_TYPE_ID as i_WM_LABOR_TYPE_ID", \
	"i_WM_LABOR_TYPE_CD as i_WM_LABOR_TYPE_CD", \
	"i_WM_LABOR_TYPE_DESC as i_WM_LABOR_TYPE_DESC", \
	"i_SPVSR_AUTH_REQUIRED_FLAG as i_SPVSR_AUTH_REQUIRED_FLAG", \
	"i_MISC_TXT_11 as i_MISC_TXT_11", \
	"i_MISC_TXT_21 as i_MISC_TXT_21", \
	"i_MISC_NUM_11 as i_MISC_NUM_11", \
	"i_MISC_NUM_21 as i_MISC_NUM_21", \
	"i_WM_USER_ID as i_WM_USER_ID", \
	"i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"i_DELETE_FLAG as i_DELETE_FLAG", \
	"i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"DELETE_FLAG as DELETE_FLAG", \
	"UPDATE_TSTMP as UPDATE_TSTMP", \
	"LOAD_TSTMP as LOAD_TSTMP", \
	"o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR" ,\
	"sys_row_id as sys_row_id").select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID1'), \
	col('LABOR_TYPE_ID').alias('LABOR_TYPE_ID1'), \
	col('LABOR_TYPE_CODE').alias('LABOR_TYPE_CODE1'), \
	col('DESCRIPTION').alias('DESCRIPTION1'), \
	col('USER_ID').alias('USER_ID1'), \
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME1'), \
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME1'), \
	col('MISC_TXT_1').alias('MISC_TXT_11'), \
	col('MISC_TXT_2').alias('MISC_TXT_21'), \
	col('MISC_NUM_1').alias('MISC_NUM_11'), \
	col('MISC_NUM_2').alias('MISC_NUM_21'), \
	col('VERSION_ID').alias('VERSION_ID1'), \
	col('SPVSR_AUTH_REQUIRED').alias('SPVSR_AUTH_REQUIRED1'), \
	col('CREATED_DTTM').alias('CREATED_DTTM1'), \
	col('LAST_UPDATED_DTTM').alias('LAST_UPDATED_DTTM1'), \
	col('i_LOCATION_ID1').alias('i_LOCATION_ID11'), \
	col('i_WM_LABOR_TYPE_ID').alias('i_WM_LABOR_TYPE_ID1'), \
	col('i_WM_LABOR_TYPE_CD').alias('i_WM_LABOR_TYPE_CD1'), \
	col('i_WM_LABOR_TYPE_DESC').alias('i_WM_LABOR_TYPE_DESC1'), \
	col('i_SPVSR_AUTH_REQUIRED_FLAG').alias('i_SPVSR_AUTH_REQUIRED_FLAG1'), \
	col('i_MISC_TXT_11').alias('i_MISC_TXT_111'), \
	col('i_MISC_TXT_21').alias('i_MISC_TXT_211'), \
	col('i_MISC_NUM_11').alias('i_MISC_NUM_111'), \
	col('i_MISC_NUM_21').alias('i_MISC_NUM_211'), \
	col('i_WM_USER_ID').alias('i_WM_USER_ID1'), \
	col('i_WM_VERSION_ID').alias('i_WM_VERSION_ID1'), \
	col('i_WM_CREATE_TSTMP').alias('i_WM_CREATE_TSTMP1'), \
	col('i_WM_MOD_TSTMP').alias('i_WM_MOD_TSTMP1'), \
	col('i_WM_CREATED_TSTMP').alias('i_WM_CREATED_TSTMP1'), \
	col('i_WM_LAST_UPDATED_TSTMP').alias('i_WM_LAST_UPDATED_TSTMP1'), \
	col('i_DELETE_FLAG').alias('i_DELETE_FLAG1'), \
	col('i_LOAD_TSTMP').alias('i_LOAD_TSTMP1'), \
	col('DELETE_FLAG').alias('DELETE_FLAG1'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP1'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP1'), \
	col('o_UPDATE_VALIDATOR').alias('o_UPDATE_VALIDATOR1')).filter("o_UPDATE_VALIDATOR = 'INSERT' OR o_UPDATE_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_INSERT_UPDATE_temp = RTR_INS_UPD_DEL_INSERT_UPDATE.toDF(*["RTR_INS_UPD_DEL_INSERT_UPDATE___" + col for col in RTR_INS_UPD_DEL_INSERT_UPDATE.columns])

UPD_INS_UPD = RTR_INS_UPD_DEL_INSERT_UPDATE_temp.selectExpr( \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LABOR_TYPE_ID1 as LABOR_TYPE_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LABOR_TYPE_CODE1 as LABOR_TYPE_CODE1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___DESCRIPTION1 as DESCRIPTION1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___USER_ID1 as USER_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATE_DATE_TIME1 as CREATE_DATE_TIME1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MOD_DATE_TIME1 as MOD_DATE_TIME1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_TXT_11 as MISC_TXT_11", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_TXT_21 as MISC_TXT_21", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_NUM_11 as MISC_NUM_11", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MISC_NUM_21 as MISC_NUM_21", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___VERSION_ID1 as VERSION_ID1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___SPVSR_AUTH_REQUIRED1 as SPVSR_AUTH_REQUIRED1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_DTTM1 as CREATED_DTTM1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_DTTM1 as LAST_UPDATED_DTTM1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___DELETE_FLAG1 as DELETE_FLAG1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP1", \
	"RTR_INS_UPD_DEL_INSERT_UPDATE___o_UPDATE_VALIDATOR1 as o_UPDATE_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR1') ==(lit('INSERT')), lit(0)).when(col('o_UPDATE_VALIDATOR1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_DELETE_temp = RTR_INS_UPD_DEL_DELETE.toDF(*["RTR_INS_UPD_DEL_DELETE___" + col for col in RTR_INS_UPD_DEL_DELETE.columns])

UPD_DELETE = RTR_INS_UPD_DEL_DELETE_temp.selectExpr( \
	"RTR_INS_UPD_DEL_DELETE___i_LOCATION_ID13 as i_LOCATION_ID13", \
	"RTR_INS_UPD_DEL_DELETE___i_WM_LABOR_TYPE_ID3 as i_WM_LABOR_TYPE_ID3", \
	"RTR_INS_UPD_DEL_DELETE___DELETE_FLAG3 as DELETE_FLAG3", \
	"RTR_INS_UPD_DEL_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3") \
	.withColumn('pyspark_data_action', lit(1))


# COMMAND ----------
# Processing node Shortcut_to_WM_E_LABOR_TYPE_CODE11, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_WM_E_LABOR_TYPE_CODE11 = UPD_DELETE.selectExpr( \
	"CAST(i_LOCATION_ID13 AS BIGINT) as LOCATION_ID", \
	"CAST(i_WM_LABOR_TYPE_ID3 AS BIGINT) as WM_LABOR_TYPE_ID", \
	"CAST(DELETE_FLAG3 AS TINYINT) as DELETE_FLAG"\
)
# Shortcut_to_WM_E_LABOR_TYPE_CODE11.write.saveAsTable(f'{raw}.WM_E_LABOR_TYPE_CODE')

Shortcut_to_WM_E_LABOR_TYPE_CODE11.createOrReplaceTempView('WM_E_LABOR_TYPE_CODE_DEL')

spark.sql(f"""
          MERGE INTO {refined_perf_table} trg
          USING WM_E_LABOR_TYPE_CODE_DEL src
          ON (src.LOCATION_ID = trg.LOCATION_ID AND src.WM_LABOR_TYPE_ID = trg.WM_LABOR_TYPE_ID )
          WHEN MATCHED THEN UPDATE SET trg.DELETE_FLAG = src.DELETE_FLAG , trg.UPDATE_TSTMP = CURRENT_TIMESTAMP()
          """)



# COMMAND ----------
# Processing node Shortcut_to_WM_E_LABOR_TYPE_CODE1, type TARGET 
# COLUMN COUNT: 18

Shortcut_to_WM_E_LABOR_TYPE_CODE1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(LABOR_TYPE_ID1 AS INT) as WM_LABOR_TYPE_ID",
	"CAST(LABOR_TYPE_CODE1 AS STRING) as WM_LABOR_TYPE_CD",
	"CAST(DESCRIPTION1 AS STRING) as WM_LABOR_TYPE_DESC",
	"CAST(SPVSR_AUTH_REQUIRED1 AS TINYINT) as SPVSR_AUTH_REQUIRED_FLAG",
	"CAST(MISC_TXT_11 AS STRING) as MISC_TXT_1",
	"CAST(MISC_TXT_21 AS STRING) as MISC_TXT_2",
	"CAST(MISC_NUM_11 AS DECIMAL(20,7)) as MISC_NUM_1",
	"CAST(MISC_NUM_21 AS DECIMAL(20,7)) as MISC_NUM_2",
	"CAST(USER_ID1 AS STRING) as WM_USER_ID",
	"CAST(VERSION_ID1 AS INT) as WM_VERSION_ID",
	"CAST(CREATE_DATE_TIME1 AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME1 AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(CREATED_DTTM1 AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM1 AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(DELETE_FLAG1 AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LABOR_TYPE_ID = target.WM_LABOR_TYPE_ID"""
#   refined_perf_table = "WM_E_LABOR_TYPE_CODE"
  executeMerge(Shortcut_to_WM_E_LABOR_TYPE_CODE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_LABOR_TYPE_CODE", "WM_E_LABOR_TYPE_CODE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_LABOR_TYPE_CODE", "WM_E_LABOR_TYPE_CODE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

