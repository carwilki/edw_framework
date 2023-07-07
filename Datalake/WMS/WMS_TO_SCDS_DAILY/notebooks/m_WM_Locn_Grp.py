#Code converted on 2023-06-26 10:17:21
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

# COMMAND ----------
pre_perf_table = f"{raw}.WM_LOCN_GRP_PRE"
refined_perf_table = f"{refine}.WM_LOCN_GRP"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table, refine,raw)
Del_Logic=args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LOCN_GRP, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_WM_LOCN_GRP = spark.sql(f"""SELECT
WM_LOCN_GRP.LOCATION_ID,
WM_LOCN_GRP.WM_LOCN_GRP_ID,
WM_LOCN_GRP.WM_GRP_TYPE,
WM_LOCN_GRP.WM_LOCN_ID,
WM_LOCN_GRP.WM_GRP_ATTR,
WM_LOCN_GRP.WM_LOCN_HDR_ID,
WM_LOCN_GRP.WM_USER_ID,
WM_LOCN_GRP.WM_VERSION_ID,
WM_LOCN_GRP.WM_CREATED_TSTMP,
WM_LOCN_GRP.WM_LAST_UPDATED_TSTMP,
WM_LOCN_GRP.WM_CREATE_TSTMP,
WM_LOCN_GRP.WM_MOD_TSTMP,
WM_LOCN_GRP.DELETE_FLAG,
WM_LOCN_GRP.UPDATE_TSTMP,
WM_LOCN_GRP.LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 

DELETE_FLAG =0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LOCN_GRP_PRE, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_WM_LOCN_GRP_PRE = spark.sql(f"""SELECT
WM_LOCN_GRP_PRE.DC_NBR,
WM_LOCN_GRP_PRE.LOCN_GRP_ID,
WM_LOCN_GRP_PRE.GRP_TYPE,
WM_LOCN_GRP_PRE.LOCN_ID,
WM_LOCN_GRP_PRE.GRP_ATTR,
WM_LOCN_GRP_PRE.CREATE_DATE_TIME,
WM_LOCN_GRP_PRE.MOD_DATE_TIME,
WM_LOCN_GRP_PRE.USER_ID,
WM_LOCN_GRP_PRE.LOCN_HDR_ID,
WM_LOCN_GRP_PRE.WM_VERSION_ID,
WM_LOCN_GRP_PRE.CREATED_DTTM,
WM_LOCN_GRP_PRE.LAST_UPDATED_DTTM
FROM {pre_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LOCN_GRP_PRE_temp = SQ_Shortcut_to_WM_LOCN_GRP_PRE.toDF(*["SQ_Shortcut_to_WM_LOCN_GRP_PRE___" + col for col in SQ_Shortcut_to_WM_LOCN_GRP_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_LOCN_GRP_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LOCN_GRP_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___LOCN_GRP_ID as LOCN_GRP_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___GRP_TYPE as GRP_TYPE", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___LOCN_ID as LOCN_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___GRP_ATTR as GRP_ATTR", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___LOCN_HDR_ID as LOCN_HDR_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___WM_VERSION_ID as WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_LOCN_GRP_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" \
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
# COLUMN COUNT: 14

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LOCN_GRP, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 27

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LOCN_GRP_temp = SQ_Shortcut_to_WM_LOCN_GRP.toDF(*["SQ_Shortcut_to_WM_LOCN_GRP___" + col for col in SQ_Shortcut_to_WM_LOCN_GRP.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_LOCN_GRP = SQ_Shortcut_to_WM_LOCN_GRP_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LOCN_GRP_temp.SQ_Shortcut_to_WM_LOCN_GRP___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LOCN_GRP_temp.SQ_Shortcut_to_WM_LOCN_GRP___WM_LOCN_GRP_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCN_GRP_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___LOCN_GRP_ID as LOCN_GRP_ID", \
	"JNR_SITE_PROFILE___GRP_TYPE as GRP_TYPE", \
	"JNR_SITE_PROFILE___LOCN_ID as LOCN_ID", \
	"JNR_SITE_PROFILE___GRP_ATTR as GRP_ATTR", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___LOCN_HDR_ID as LOCN_HDR_ID", \
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_LOCN_GRP___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_LOCN_GRP_ID as i_WM_LOCN_GRP_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_GRP_TYPE as WM_GRP_TYPE", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_LOCN_ID as WM_LOCN_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_GRP_ATTR as WM_GRP_ATTR", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_USER_ID as WM_USER_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_VERSION_ID as i_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_GRP___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_GRP___DELETE_FLAG as i_WM_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_LOCN_GRP___UPDATE_TSTMP as i_WM_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_GRP___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 27

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LOCN_GRP_temp = JNR_WM_LOCN_GRP.toDF(*["JNR_WM_LOCN_GRP___" + col for col in JNR_WM_LOCN_GRP.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_LOCN_GRP_temp.selectExpr( \
	"JNR_WM_LOCN_GRP___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LOCN_GRP___LOCN_GRP_ID as LOCN_GRP_ID", \
	"JNR_WM_LOCN_GRP___GRP_TYPE as GRP_TYPE", \
	"JNR_WM_LOCN_GRP___LOCN_ID as LOCN_ID", \
	"JNR_WM_LOCN_GRP___GRP_ATTR as GRP_ATTR", \
	"JNR_WM_LOCN_GRP___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_LOCN_GRP___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_LOCN_GRP___USER_ID as USER_ID", \
	"JNR_WM_LOCN_GRP___LOCN_HDR_ID as LOCN_HDR_ID", \
	"JNR_WM_LOCN_GRP___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_WM_LOCN_GRP___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_LOCN_GRP___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_LOCN_GRP___WM_LOCN_ID as WM_LOCN_ID", \
	"JNR_WM_LOCN_GRP___i_WM_LOCN_GRP_ID as i_WM_LOCN_GRP_ID", \
	"JNR_WM_LOCN_GRP___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_LOCN_GRP___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_LOCN_GRP___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_LOCN_GRP___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_LOCN_GRP___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"JNR_WM_LOCN_GRP___i_WM_UPDATE_TSTMP as i_WM_UPDATE_TSTMP", \
	"JNR_WM_LOCN_GRP___i_LOCATION_ID as i_LOCATION_ID", \
	"JNR_WM_LOCN_GRP___WM_GRP_TYPE as WM_GRP_TYPE", \
	"JNR_WM_LOCN_GRP___WM_GRP_ATTR as WM_GRP_ATTR", \
	"JNR_WM_LOCN_GRP___WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", \
	"JNR_WM_LOCN_GRP___WM_USER_ID as WM_USER_ID", \
	"JNR_WM_LOCN_GRP___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"JNR_WM_LOCN_GRP___i_WM_DELETE_FLAG as i_WM_DELETE_FLAG")\
    .filter("i_WM_LOCN_GRP_ID is Null OR LOCN_GRP_ID is Null OR (  i_WM_LOCN_GRP_ID is NOT Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01') OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01') OR COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])\
.withColumn("v_CREATE_DATE_TIME", expr("""IF (CREATE_DATE_TIME IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), CREATE_DATE_TIME)""")) \
	.withColumn("v_MOD_DATE_TIME", expr("""IF (MOD_DATE_TIME IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), MOD_DATE_TIME)""")) \
	.withColumn("v_CREATED_DTTM", expr("""IF (CREATED_DTTM IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), CREATED_DTTM)""")) \
	.withColumn("v_LAST_UPDATED_DTTM", expr("""IF (LAST_UPDATED_DTTM IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), LAST_UPDATED_DTTM)""")) \
	.withColumn("v_i_WM_CREATED_TSTMP", expr("""IF (i_WM_CREATED_TSTMP IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), i_WM_CREATED_TSTMP)""")) \
	.withColumn("v_i_WM_LAST_UPDATED_TSTMP", expr("""IF (i_WM_LAST_UPDATED_TSTMP IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), i_WM_LAST_UPDATED_TSTMP)""")) \
	.withColumn("v_i_WM_CREATE_TSTMP", expr("""IF (i_WM_CREATE_TSTMP IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), i_WM_CREATE_TSTMP)""")) \
	.withColumn("v_i_WM_MOD_TSTMP", expr("""IF (i_WM_MOD_TSTMP IS NULL, TO_DATE ( '01/01/1900' , 'MM/DD/YYYY' ), i_WM_MOD_TSTMP)"""))

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___LOCN_GRP_ID as LOCN_GRP_ID", \
	"FIL_UNCHANGED_RECORDS___GRP_TYPE as GRP_TYPE", \
	"FIL_UNCHANGED_RECORDS___LOCN_ID as LOCN_ID", \
	"FIL_UNCHANGED_RECORDS___GRP_ATTR as GRP_ATTR", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", \
	"FIL_UNCHANGED_RECORDS___LOCN_HDR_ID as LOCN_HDR_ID", \
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID as WM_VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___i_WM_LOCN_GRP_ID as i_WM_LOCN_GRP_ID", \
	"FIL_UNCHANGED_RECORDS___WM_LOCN_ID as WM_LOCN_ID", \
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID as i_LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___WM_GRP_TYPE as WM_GRP_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_GRP_ATTR as WM_GRP_ATTR", \
	"FIL_UNCHANGED_RECORDS___WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", \
	"FIL_UNCHANGED_RECORDS___WM_USER_ID as WM_USER_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_DELETE_FLAG as i_WM_DELETE_FLAG", \
	"IF (FIL_UNCHANGED_RECORDS___LOCN_GRP_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_LOCN_GRP_ID IS NOT NULL, 1, 0) as DELETE_FLAG_EXP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___LOCN_GRP_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_LOCN_GRP_ID IS NULL, 'INSERT', IF (FIL_UNCHANGED_RECORDS___LOCN_GRP_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_LOCN_GRP_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_CREATE_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATE_DATE_TIME OR FIL_UNCHANGED_RECORDS___v_i_WM_MOD_TSTMP <> FIL_UNCHANGED_RECORDS___v_MOD_DATE_TIME ), 'UPDATE', IF (FIL_UNCHANGED_RECORDS___LOCN_GRP_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_LOCN_GRP_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_RECORDS___v_i_WM_CREATE_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_RECORDS___v_i_WM_MOD_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) ), 'DELETE', NULL))) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node RTR_DELETE, type ROUTER 
# COLUMN COUNT: 30


# Creating output dataframe for RTR_DELETE, output group DELETE
RTR_DELETE_DELETE = EXP_UPDATE_VALIDATOR.select(EXP_UPDATE_VALIDATOR.sys_row_id.alias('sys_row_id'), \
	EXP_UPDATE_VALIDATOR.LOCATION_ID.alias('LOCATION_ID3'), \
	EXP_UPDATE_VALIDATOR.LOCN_GRP_ID.alias('LOCN_GRP_ID3'), \
	EXP_UPDATE_VALIDATOR.GRP_TYPE.alias('GRP_TYPE3'), \
	EXP_UPDATE_VALIDATOR.LOCN_ID.alias('LOCN_ID3'), \
	EXP_UPDATE_VALIDATOR.GRP_ATTR.alias('GRP_ATTR3'), \
	EXP_UPDATE_VALIDATOR.CREATE_DATE_TIME.alias('CREATE_DATE_TIME3'), \
	EXP_UPDATE_VALIDATOR.MOD_DATE_TIME.alias('MOD_DATE_TIME3'), \
	EXP_UPDATE_VALIDATOR.USER_ID.alias('USER_ID3'), \
	EXP_UPDATE_VALIDATOR.LOCN_HDR_ID.alias('LOCN_HDR_ID3'), \
	EXP_UPDATE_VALIDATOR.WM_VERSION_ID.alias('WM_VERSION_ID3'), \
	EXP_UPDATE_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM3'), \
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM3'), \
	EXP_UPDATE_VALIDATOR.i_WM_LOCN_GRP_ID.alias('i_WM_LOCN_GRP_ID3'), \
	EXP_UPDATE_VALIDATOR.i_LOAD_TSTMP.alias('i_LOAD_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.DELETE_FLAG_EXP.alias('DELETE_FLAG_EXP3'), \
	EXP_UPDATE_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR3'), \
	EXP_UPDATE_VALIDATOR.i_LOCATION_ID.alias('i_LOCATION_ID3'), \
	EXP_UPDATE_VALIDATOR.WM_GRP_TYPE.alias('WM_GRP_TYPE3'), \
	EXP_UPDATE_VALIDATOR.WM_GRP_ATTR.alias('WM_GRP_ATTR3'), \
	EXP_UPDATE_VALIDATOR.WM_LOCN_HDR_ID.alias('WM_LOCN_HDR_ID3'), \
	EXP_UPDATE_VALIDATOR.WM_USER_ID.alias('WM_USER_ID3'), \
	EXP_UPDATE_VALIDATOR.i_WM_VERSION_ID.alias('i_WM_VERSION_ID3'), \
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.i_WM_CREATE_TSTMP.alias('i_WM_CREATE_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.i_WM_MOD_TSTMP.alias('i_WM_MOD_TSTMP3'), \
	EXP_UPDATE_VALIDATOR.i_WM_DELETE_FLAG.alias('i_WM_DELETE_FLAG3'), \
	EXP_UPDATE_VALIDATOR.WM_LOCN_ID.alias('WM_LOCN_ID3')).filter("o_UPDATE_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_DELETE, output group INSERT_UPDATE
RTR_DELETE_INSERT_UPDATE = EXP_UPDATE_VALIDATOR.select(EXP_UPDATE_VALIDATOR.sys_row_id.alias('sys_row_id'), \
	EXP_UPDATE_VALIDATOR.LOCATION_ID.alias('LOCATION_ID1'), \
	EXP_UPDATE_VALIDATOR.LOCN_GRP_ID.alias('LOCN_GRP_ID1'), \
	EXP_UPDATE_VALIDATOR.GRP_TYPE.alias('GRP_TYPE1'), \
	EXP_UPDATE_VALIDATOR.LOCN_ID.alias('LOCN_ID1'), \
	EXP_UPDATE_VALIDATOR.GRP_ATTR.alias('GRP_ATTR1'), \
	EXP_UPDATE_VALIDATOR.CREATE_DATE_TIME.alias('CREATE_DATE_TIME1'), \
	EXP_UPDATE_VALIDATOR.MOD_DATE_TIME.alias('MOD_DATE_TIME1'), \
	EXP_UPDATE_VALIDATOR.USER_ID.alias('USER_ID1'), \
	EXP_UPDATE_VALIDATOR.LOCN_HDR_ID.alias('LOCN_HDR_ID1'), \
	EXP_UPDATE_VALIDATOR.WM_VERSION_ID.alias('WM_VERSION_ID1'), \
	EXP_UPDATE_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM1'), \
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM1'), \
	EXP_UPDATE_VALIDATOR.i_WM_LOCN_GRP_ID.alias('i_WM_LOCN_GRP_ID1'), \
	EXP_UPDATE_VALIDATOR.i_LOAD_TSTMP.alias('i_LOAD_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.DELETE_FLAG_EXP.alias('DELETE_FLAG_EXP1'), \
	EXP_UPDATE_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR1'), \
	EXP_UPDATE_VALIDATOR.i_LOCATION_ID.alias('i_LOCATION_ID1'), \
	EXP_UPDATE_VALIDATOR.WM_GRP_TYPE.alias('WM_GRP_TYPE1'), \
	EXP_UPDATE_VALIDATOR.WM_GRP_ATTR.alias('WM_GRP_ATTR1'), \
	EXP_UPDATE_VALIDATOR.WM_LOCN_HDR_ID.alias('WM_LOCN_HDR_ID1'), \
	EXP_UPDATE_VALIDATOR.WM_USER_ID.alias('WM_USER_ID1'), \
	EXP_UPDATE_VALIDATOR.i_WM_VERSION_ID.alias('i_WM_VERSION_ID1'), \
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.i_WM_CREATE_TSTMP.alias('i_WM_CREATE_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.i_WM_MOD_TSTMP.alias('i_WM_MOD_TSTMP1'), \
	EXP_UPDATE_VALIDATOR.i_WM_DELETE_FLAG.alias('i_WM_DELETE_FLAG1'), \
	EXP_UPDATE_VALIDATOR.WM_LOCN_ID.alias('WM_LOCN_ID1')).filter("o_UPDATE_VALIDATOR = 'INSERT' OR o_UPDATE_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_DELETE_temp = RTR_DELETE_DELETE.toDF(*["RTR_DELETE_DELETE___" + col for col in RTR_DELETE_DELETE.columns])

UPD_DELETE = RTR_DELETE_DELETE_temp.selectExpr( \
	"RTR_DELETE_DELETE___i_LOCATION_ID3 as i_LOCATION_ID3", \
	"RTR_DELETE_DELETE___i_WM_LOCN_GRP_ID3 as i_WM_LOCN_GRP_ID3", \
	"RTR_DELETE_DELETE___DELETE_FLAG_EXP3 as DELETE_FLAG_EXP", \
	"RTR_DELETE_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_LOCN_GRP1, type TARGET 
# COLUMN COUNT: 15


# Shortcut_to_WM_LOCN_GRP1 = UPD_DELETE.selectExpr( \
# 	"CAST(i_LOCATION_ID3 AS BIGINT) as LOCATION_ID", \
# 	"CAST(i_WM_LOCN_GRP_ID3 AS BIGINT) as WM_LOCN_GRP_ID", \
# 	"CAST(NULL AS BIGINT) as WM_GRP_TYPE", \
# 	"CAST(NULL AS STRING) as WM_LOCN_ID", \
# 	"CAST(NULL AS STRING) as WM_GRP_ATTR", \
# 	"CAST(NULL AS BIGINT) as WM_LOCN_HDR_ID", \
# 	"CAST(NULL AS STRING) as WM_USER_ID", \
# 	"CAST(NULL AS BIGINT) as WM_VERSION_ID", \
# 	"CAST(NULL AS TIMESTAMP) as WM_CREATED_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as WM_CREATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as WM_MOD_TSTMP", \
# 	"CAST(DELETE_FLAG_EXP AS BIGINT) as DELETE_FLAG", \
# 	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP" \
# )
# Shortcut_to_WM_LOCN_GRP1.write.saveAsTable(f'{raw}.WM_LOCN_GRP')

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_INSERT_UPDATE_temp = RTR_DELETE_INSERT_UPDATE.toDF(*["RTR_DELETE_INSERT_UPDATE___" + col for col in RTR_DELETE_INSERT_UPDATE.columns])

UPD_INSERT_UPDATE = RTR_DELETE_INSERT_UPDATE_temp.selectExpr( \
	"RTR_DELETE_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID", \
	"RTR_DELETE_INSERT_UPDATE___LOCN_GRP_ID1 as LOCN_GRP_ID", \
	"RTR_DELETE_INSERT_UPDATE___GRP_TYPE1 as GRP_TYPE", \
	"RTR_DELETE_INSERT_UPDATE___LOCN_ID1 as LOCN_ID", \
	"RTR_DELETE_INSERT_UPDATE___GRP_ATTR1 as GRP_ATTR", \
	"RTR_DELETE_INSERT_UPDATE___LOCN_HDR_ID1 as LOCN_HDR_ID", \
	"RTR_DELETE_INSERT_UPDATE___USER_ID1 as USER_ID", \
	"RTR_DELETE_INSERT_UPDATE___WM_VERSION_ID1 as WM_VERSION_ID", \
	"RTR_DELETE_INSERT_UPDATE___CREATED_DTTM1 as CREATED_DTTM", \
	"RTR_DELETE_INSERT_UPDATE___LAST_UPDATED_DTTM1 as LAST_UPDATED_DTTM", \
	"RTR_DELETE_INSERT_UPDATE___CREATE_DATE_TIME1 as CREATE_DATE_TIME", \
	"RTR_DELETE_INSERT_UPDATE___MOD_DATE_TIME1 as MOD_DATE_TIME", \
	"RTR_DELETE_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP", \
	"RTR_DELETE_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP", \
	"RTR_DELETE_INSERT_UPDATE___o_UPDATE_VALIDATOR1 as o_UPDATE_VALIDATOR1", \
	"RTR_DELETE_INSERT_UPDATE___DELETE_FLAG_EXP1 as DELETE_FLAG_EXP") \
	.withColumn('pyspark_data_action', when(RTR_DELETE_INSERT_UPDATE.o_UPDATE_VALIDATOR1 ==(lit('INSERT')) , lit(0)).when(RTR_DELETE_INSERT_UPDATE.o_UPDATE_VALIDATOR1 ==(lit('UPDATE')) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LOCN_GRP2, type TARGET 
# COLUMN COUNT: 15

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LOCN_GRP_ID = target.WM_LOCN_GRP_ID"""
#   refined_perf_table = "WM_LOCN_GRP"
  executeMerge(UPD_INSERT_UPDATE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LOCN_GRP", "WM_LOCN_GRP", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LOCN_GRP", "WM_LOCN_GRP","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	