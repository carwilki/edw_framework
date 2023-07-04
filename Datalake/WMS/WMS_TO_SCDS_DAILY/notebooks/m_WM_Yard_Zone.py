#Code converted on 2023-06-22 21:04:43
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

# Read in relation source variables
# (username, password, connection_string) = getConfig(DC_NBR, env)

# COMMAND ----------
# Variable_declaration_comment
Prev_Run_Dt=args.Prev_Run_Dt
Del_Logic=args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_YARD_ZONE_PRE, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_WM_YARD_ZONE_PRE = spark.sql(f"""SELECT
WM_YARD_ZONE_PRE.DC_NBR,
WM_YARD_ZONE_PRE.YARD_ID,
WM_YARD_ZONE_PRE.YARD_ZONE_ID,
WM_YARD_ZONE_PRE.YARD_ZONE_NAME,
WM_YARD_ZONE_PRE.MARK_FOR_DELETION,
WM_YARD_ZONE_PRE.PUTAWAY_ELIGIBLE,
WM_YARD_ZONE_PRE.LOCATION_ID,
WM_YARD_ZONE_PRE.CREATED_DTTM,
WM_YARD_ZONE_PRE.LAST_UPDATED_DTTM,
WM_YARD_ZONE_PRE.CREATED_SOURCE,
WM_YARD_ZONE_PRE.CREATED_SOURCE_TYPE,
WM_YARD_ZONE_PRE.LAST_UPDATED_SOURCE,
WM_YARD_ZONE_PRE.LAST_UPDATED_SOURCE_TYPE
FROM WM_YARD_ZONE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_YARD_ZONE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_WM_YARD_ZONE = spark.sql(f"""SELECT
WM_YARD_ZONE.LOCATION_ID,
WM_YARD_ZONE.WM_YARD_ID,
WM_YARD_ZONE.WM_YARD_ZONE_ID,
WM_YARD_ZONE.WM_YARD_ZONE_NAME,
WM_YARD_ZONE.WM_LOCATION_ID,
WM_YARD_ZONE.PUTAWAY_ELIGIBLE_FLAG,
WM_YARD_ZONE.MARK_FOR_DELETION_FLAG,
WM_YARD_ZONE.WM_CREATED_SOURCE_TYPE,
WM_YARD_ZONE.WM_CREATED_SOURCE,
WM_YARD_ZONE.WM_CREATED_TSTMP,
WM_YARD_ZONE.WM_LAST_UPDATED_SOURCE_TYPE,
WM_YARD_ZONE.WM_LAST_UPDATED_SOURCE,
WM_YARD_ZONE.WM_LAST_UPDATED_TSTMP,
WM_YARD_ZONE.DELETE_FLAG,
WM_YARD_ZONE.LOAD_TSTMP
FROM WM_YARD_ZONE
WHERE {Del_Logic} 1=0 and DELETE_FLAG =0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_YARD_ZONE_PRE_temp = SQ_Shortcut_to_WM_YARD_ZONE_PRE.toDF(*["SQ_Shortcut_to_WM_YARD_ZONE_PRE___" + col for col in SQ_Shortcut_to_WM_YARD_ZONE_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_YARD_ZONE_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_YARD_ZONE_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___YARD_ID as YARD_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___YARD_ZONE_ID as YARD_ZONE_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___YARD_ZONE_NAME as YARD_ZONE_NAME", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___PUTAWAY_ELIGIBLE as PUTAWAY_ELIGIBLE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___LOCATION_ID as LOCATION_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONV_temp = EXP_INT_CONV.toDF(*["EXP_INT_CONV___" + col for col in EXP_INT_CONV.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = EXP_INT_CONV_temp.join(SQ_Shortcut_to_SITE_PROFILE_temp,[EXP_INT_CONV_temp.EXP_INT_CONV___o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR],'inner').selectExpr( 
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR", 
	"EXP_INT_CONV___o_DC_NBR as o_DC_NBR", 
	"EXP_INT_CONV___YARD_ID as YARD_ID", 
	"EXP_INT_CONV___YARD_ZONE_ID as YARD_ZONE_ID", 
	"EXP_INT_CONV___YARD_ZONE_NAME as YARD_ZONE_NAME", 
	"EXP_INT_CONV___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"EXP_INT_CONV___PUTAWAY_ELIGIBLE as PUTAWAY_ELIGIBLE", 
	"EXP_INT_CONV___LOCATION_ID as LOCATION_ID1", 
	"EXP_INT_CONV___CREATED_DTTM as CREATED_DTTM", 
	"EXP_INT_CONV___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_INT_CONV___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_INT_CONV___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"EXP_INT_CONV___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_INT_CONV___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE")

# COMMAND ----------
# Processing node JNR_WM_YARD_ZONE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_YARD_ZONE_temp = SQ_Shortcut_to_WM_YARD_ZONE.toDF(*["SQ_Shortcut_to_WM_YARD_ZONE___" + col for col in SQ_Shortcut_to_WM_YARD_ZONE.columns])

JNR_WM_YARD_ZONE = SQ_Shortcut_to_WM_YARD_ZONE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_YARD_ZONE_temp.SQ_Shortcut_to_WM_YARD_ZONE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_YARD_ZONE_temp.SQ_Shortcut_to_WM_YARD_ZONE___WM_YARD_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___YARD_ID, SQ_Shortcut_to_WM_YARD_ZONE_temp.SQ_Shortcut_to_WM_YARD_ZONE___WM_YARD_ZONE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___YARD_ZONE_ID],'fullouter').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___YARD_ID as YARD_ID", 
	"JNR_SITE_PROFILE___YARD_ZONE_ID as YARD_ZONE_ID", 
	"JNR_SITE_PROFILE___YARD_ZONE_NAME as YARD_ZONE_NAME", 
	"JNR_SITE_PROFILE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_SITE_PROFILE___PUTAWAY_ELIGIBLE as PUTAWAY_ELIGIBLE", 
	"JNR_SITE_PROFILE___LOCATION_ID1 as LOCATION_ID1", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE___LOCATION_ID as i_LOCATION_ID2", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_YARD_ID as i_WM_YARD_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_YARD_ZONE_ID as i_WM_YARD_ZONE_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_YARD_ZONE_NAME as i_WM_YARD_ZONE_NAME", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_LOCATION_ID as i_WM_LOCATION_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE___PUTAWAY_ELIGIBLE_FLAG as i_PUTAWAY_ELIGIBLE_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE___MARK_FOR_DELETION_FLAG as i_MARK_FOR_DELETION_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_YARD_ZONE___DELETE_FLAG as i_DELETE_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_YARD_ZONE_temp = JNR_WM_YARD_ZONE.toDF(*["JNR_WM_YARD_ZONE___" + col for col in JNR_WM_YARD_ZONE.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_YARD_ZONE_temp.selectExpr( 
	"JNR_WM_YARD_ZONE___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_YARD_ZONE___YARD_ID as YARD_ID", 
	"JNR_WM_YARD_ZONE___YARD_ZONE_ID as YARD_ZONE_ID", 
	"JNR_WM_YARD_ZONE___YARD_ZONE_NAME as YARD_ZONE_NAME", 
	"JNR_WM_YARD_ZONE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_WM_YARD_ZONE___PUTAWAY_ELIGIBLE as PUTAWAY_ELIGIBLE", 
	"JNR_WM_YARD_ZONE___LOCATION_ID1 as LOCATION_ID1", 
	"JNR_WM_YARD_ZONE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_YARD_ZONE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_YARD_ZONE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_WM_YARD_ZONE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_WM_YARD_ZONE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE___i_LOCATION_ID2 as i_LOCATION_ID2", 
	"JNR_WM_YARD_ZONE___i_WM_YARD_ID as i_WM_YARD_ID", 
	"JNR_WM_YARD_ZONE___i_WM_YARD_ZONE_ID as i_WM_YARD_ZONE_ID", 
	"JNR_WM_YARD_ZONE___i_WM_YARD_ZONE_NAME as i_WM_YARD_ZONE_NAME", 
	"JNR_WM_YARD_ZONE___i_WM_LOCATION_ID as i_WM_LOCATION_ID", 
	"JNR_WM_YARD_ZONE___i_PUTAWAY_ELIGIBLE_FLAG as i_PUTAWAY_ELIGIBLE_FLAG", 
	"JNR_WM_YARD_ZONE___i_MARK_FOR_DELETION_FLAG as i_MARK_FOR_DELETION_FLAG", 
	"JNR_WM_YARD_ZONE___i_WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE___i_WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"JNR_WM_YARD_ZONE___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_YARD_ZONE___i_WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE___i_WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"JNR_WM_YARD_ZONE___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_YARD_ZONE___i_DELETE_FLAG as i_DELETE_FLAG", 
	"JNR_WM_YARD_ZONE___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("YARD_ZONE_ID IS NULL OR i_WM_YARD_ZONE_ID IS NULL OR (NOT i_WM_YARD_ZONE_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01'))))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
    .withColumn("v_CREATED_DTTM", expr("""IF (CREATED_DTTM IS NULL, date'1900-01-01', CREATED_DTTM)""")) \
	.withColumn("v_LAST_UPDATED_DTTM", expr("""IF (LAST_UPDATED_DTTM IS NULL, date'1900-01-01', LAST_UPDATED_DTTM)""")) \
	.withColumn("v_i_WM_CREATED_TSTMP", expr("""IF (i_WM_CREATED_TSTMP IS NULL, date'1900-01-01', i_WM_CREATED_TSTMP)""")) \
	.withColumn("v_i_WM_LAST_UPDATED_TSTMP", expr("""IF (i_WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', i_WM_LAST_UPDATED_TSTMP)"""))
    
EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ID as YARD_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ZONE_ID as YARD_ZONE_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ZONE_NAME as YARD_ZONE_NAME", 
	"FIL_UNCHANGED_RECORDS___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"FIL_UNCHANGED_RECORDS___PUTAWAY_ELIGIBLE as PUTAWAY_ELIGIBLE", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID2 as i_LOCATION_ID2", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ID as i_WM_YARD_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_ID as i_WM_YARD_ZONE_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_NAME as i_WM_YARD_ZONE_NAME", 
	"FIL_UNCHANGED_RECORDS___i_WM_LOCATION_ID as i_WM_LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___i_PUTAWAY_ELIGIBLE_FLAG as i_PUTAWAY_ELIGIBLE_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_MARK_FOR_DELETION_FLAG as i_MARK_FOR_DELETION_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_DELETE_FLAG as i_DELETE_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___YARD_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ID IS NOT NULL, 1, 0) as DELETE_FLAG", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___YARD_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ID IS NULL, 'INSERT', IF (FIL_UNCHANGED_RECORDS___YARD_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP >= DATE_ADD(- 14, {Prev_Run_Dt}) ), 'DELETE', IF (FIL_UNCHANGED_RECORDS___YARD_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM ), 'UPDATE', NULL))) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node RTR_INS_UPD_DEL, type ROUTER 
# COLUMN COUNT: 32


# Creating output dataframe for RTR_INS_UPD_DEL, output group DELETE
RTR_INS_UPD_DEL_DELETE = EXP_UPDATE_VALIDATOR.select(EXP_UPDATE_VALIDATOR.sys_row_id.alias('sys_row_id'), 
	EXP_UPDATE_VALIDATOR.LOCATION_ID.alias('LOCATION_ID4'), 
	EXP_UPDATE_VALIDATOR.YARD_ID.alias('YARD_ID3'), 
	EXP_UPDATE_VALIDATOR.YARD_ZONE_ID.alias('YARD_ZONE_ID3'), 
	EXP_UPDATE_VALIDATOR.YARD_ZONE_NAME.alias('YARD_ZONE_NAME3'), 
	EXP_UPDATE_VALIDATOR.MARK_FOR_DELETION.alias('MARK_FOR_DELETION3'), 
	EXP_UPDATE_VALIDATOR.PUTAWAY_ELIGIBLE.alias('PUTAWAY_ELIGIBLE3'), 
	EXP_UPDATE_VALIDATOR.LOCATION_ID1.alias('LOCATION_ID13'), 
	EXP_UPDATE_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM3'), 
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM3'), 
	EXP_UPDATE_VALIDATOR.CREATED_SOURCE.alias('CREATED_SOURCE3'), 
	EXP_UPDATE_VALIDATOR.CREATED_SOURCE_TYPE.alias('CREATED_SOURCE_TYPE3'), 
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE3'), 
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_SOURCE_TYPE.alias('LAST_UPDATED_SOURCE_TYPE3'), 
	EXP_UPDATE_VALIDATOR.i_LOCATION_ID2.alias('i_LOCATION_ID23'), 
	EXP_UPDATE_VALIDATOR.i_WM_YARD_ID.alias('i_WM_YARD_ID3'), 
	EXP_UPDATE_VALIDATOR.i_WM_YARD_ZONE_ID.alias('i_WM_YARD_ZONE_ID3'), 
	EXP_UPDATE_VALIDATOR.i_WM_YARD_ZONE_NAME.alias('i_WM_YARD_ZONE_NAME3'), 
	EXP_UPDATE_VALIDATOR.i_WM_LOCATION_ID.alias('i_WM_LOCATION_ID3'), 
	EXP_UPDATE_VALIDATOR.i_PUTAWAY_ELIGIBLE_FLAG.alias('i_PUTAWAY_ELIGIBLE_FLAG3'), 
	EXP_UPDATE_VALIDATOR.i_MARK_FOR_DELETION_FLAG.alias('i_MARK_FOR_DELETION_FLAG3'), 
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_SOURCE_TYPE.alias('i_WM_CREATED_SOURCE_TYPE3'), 
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_SOURCE.alias('i_WM_CREATED_SOURCE3'), 
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP3'), 
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_SOURCE_TYPE.alias('i_WM_LAST_UPDATED_SOURCE_TYPE3'), 
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_SOURCE.alias('i_WM_LAST_UPDATED_SOURCE3'), 
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP3'), 
	EXP_UPDATE_VALIDATOR.i_DELETE_FLAG.alias('i_DELETE_FLAG3'), 
	EXP_UPDATE_VALIDATOR.i_LOAD_TSTMP.alias('i_LOAD_TSTMP3'), 
	EXP_UPDATE_VALIDATOR.DELETE_FLAG.alias('DELETE_FLAG3'), 
	EXP_UPDATE_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP3'), 
	EXP_UPDATE_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP3'), 
	EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR3')).filter("o_UPDATE_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_INS_UPD_DEL, output group INSERT_UPDATE
RTR_INS_UPD_DEL_INSERT_UPDATE = EXP_UPDATE_VALIDATOR.select(EXP_UPDATE_VALIDATOR.sys_row_id.alias('sys_row_id'), 
	EXP_UPDATE_VALIDATOR.LOCATION_ID.alias('LOCATION_ID2'), 
	EXP_UPDATE_VALIDATOR.YARD_ID.alias('YARD_ID1'), 
	EXP_UPDATE_VALIDATOR.YARD_ZONE_ID.alias('YARD_ZONE_ID1'), 
	EXP_UPDATE_VALIDATOR.YARD_ZONE_NAME.alias('YARD_ZONE_NAME1'), 
	EXP_UPDATE_VALIDATOR.MARK_FOR_DELETION.alias('MARK_FOR_DELETION1'), 
	EXP_UPDATE_VALIDATOR.PUTAWAY_ELIGIBLE.alias('PUTAWAY_ELIGIBLE1'), 
	EXP_UPDATE_VALIDATOR.LOCATION_ID1.alias('LOCATION_ID11'), 
	EXP_UPDATE_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM1'), 
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM1'), 
	EXP_UPDATE_VALIDATOR.CREATED_SOURCE.alias('CREATED_SOURCE1'), 
	EXP_UPDATE_VALIDATOR.CREATED_SOURCE_TYPE.alias('CREATED_SOURCE_TYPE1'), 
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE1'), 
	EXP_UPDATE_VALIDATOR.LAST_UPDATED_SOURCE_TYPE.alias('LAST_UPDATED_SOURCE_TYPE1'), 
	EXP_UPDATE_VALIDATOR.i_LOCATION_ID2.alias('i_LOCATION_ID21'), 
	EXP_UPDATE_VALIDATOR.i_WM_YARD_ID.alias('i_WM_YARD_ID1'), 
	EXP_UPDATE_VALIDATOR.i_WM_YARD_ZONE_ID.alias('i_WM_YARD_ZONE_ID1'), 
	EXP_UPDATE_VALIDATOR.i_WM_YARD_ZONE_NAME.alias('i_WM_YARD_ZONE_NAME1'), 
	EXP_UPDATE_VALIDATOR.i_WM_LOCATION_ID.alias('i_WM_LOCATION_ID1'), 
	EXP_UPDATE_VALIDATOR.i_PUTAWAY_ELIGIBLE_FLAG.alias('i_PUTAWAY_ELIGIBLE_FLAG1'), 
	EXP_UPDATE_VALIDATOR.i_MARK_FOR_DELETION_FLAG.alias('i_MARK_FOR_DELETION_FLAG1'), 
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_SOURCE_TYPE.alias('i_WM_CREATED_SOURCE_TYPE1'), 
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_SOURCE.alias('i_WM_CREATED_SOURCE1'), 
	EXP_UPDATE_VALIDATOR.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP1'), 
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_SOURCE_TYPE.alias('i_WM_LAST_UPDATED_SOURCE_TYPE1'), 
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_SOURCE.alias('i_WM_LAST_UPDATED_SOURCE1'), 
	EXP_UPDATE_VALIDATOR.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP1'), 
	EXP_UPDATE_VALIDATOR.i_DELETE_FLAG.alias('i_DELETE_FLAG1'), 
	EXP_UPDATE_VALIDATOR.i_LOAD_TSTMP.alias('i_LOAD_TSTMP1'), 
	EXP_UPDATE_VALIDATOR.DELETE_FLAG.alias('DELETE_FLAG1'), 
	EXP_UPDATE_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP1'), 
	EXP_UPDATE_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP1'), 
	EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR1')).filter("o_UPDATE_VALIDATOR = 'INSERT' OR o_UPDATE_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_INSERT_UPDATE_temp = RTR_INS_UPD_DEL_INSERT_UPDATE.toDF(*["RTR_INS_UPD_DEL_INSERT_UPDATE___" + col for col in RTR_INS_UPD_DEL_INSERT_UPDATE.columns])

UPD_INS_UPD = RTR_INS_UPD_DEL_INSERT_UPDATE_temp.selectExpr( 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOCATION_ID2 as LOCATION_ID2", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ID1 as YARD_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ZONE_ID1 as YARD_ZONE_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ZONE_NAME1 as YARD_ZONE_NAME1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MARK_FOR_DELETION1 as MARK_FOR_DELETION1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___PUTAWAY_ELIGIBLE1 as PUTAWAY_ELIGIBLE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOCATION_ID11 as LOCATION_ID11", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_DTTM1 as CREATED_DTTM1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_DTTM1 as LAST_UPDATED_DTTM1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_SOURCE1 as CREATED_SOURCE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_SOURCE_TYPE1 as CREATED_SOURCE_TYPE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_SOURCE1 as LAST_UPDATED_SOURCE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_SOURCE_TYPE1 as LAST_UPDATED_SOURCE_TYPE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___DELETE_FLAG1 as DELETE_FLAG1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___o_UPDATE_VALIDATOR1 as o_UPDATE_VALIDATOR1"
).withColumn('pyspark_data_action', when(RTR_INS_UPD_DEL_INSERT_UPDATE.o_UPDATE_VALIDATOR1 ==(lit('INSERT')) , lit(0)).when(RTR_INS_UPD_DEL_INSERT_UPDATE.o_UPDATE_VALIDATOR1 ==(lit('UPDATE')) , lit(1)))

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_DELETE_temp = RTR_INS_UPD_DEL_DELETE.toDF(*["RTR_INS_UPD_DEL_DELETE___" + col for col in RTR_INS_UPD_DEL_DELETE.columns])

UPD_DELETE = RTR_INS_UPD_DEL_DELETE_temp.selectExpr( 
	"RTR_INS_UPD_DEL_DELETE___i_LOCATION_ID23 as i_LOCATION_ID23", 
	"RTR_INS_UPD_DEL_DELETE___i_WM_YARD_ID3 as i_WM_YARD_ID3", 
	"RTR_INS_UPD_DEL_DELETE___i_WM_YARD_ZONE_ID3 as i_WM_YARD_ZONE_ID3", 
	"RTR_INS_UPD_DEL_DELETE___DELETE_FLAG3 as DELETE_FLAG3", 
	"RTR_INS_UPD_DEL_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3"
).withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD_ZONE1, type TARGET 
# COLUMN COUNT: 16

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_YARD_ID = target.WM_YARD_ID AND source.WM_YARD_ZONE_ID = target.WM_YARD_ZONE_ID"""
  refined_perf_table = "WM_YARD_ZONE"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_YARD_ZONE", "WM_YARD_ZONE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_YARD_ZONE", "WM_YARD_ZONE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD_ZONE11, type TARGET 
# COLUMN COUNT: 16


# Shortcut_to_WM_YARD_ZONE11 = UPD_DELETE.selectExpr( 
# 	"CAST(i_LOCATION_ID23 AS BIGINT) as LOCATION_ID", 
# 	"CAST(i_WM_YARD_ID3 AS BIGINT) as WM_YARD_ID", 
# 	"CAST(i_WM_YARD_ZONE_ID3 AS BIGINT) as WM_YARD_ZONE_ID", 
# 	"CAST(NULL AS STRING) as WM_YARD_ZONE_NAME", 
# 	"CAST(NULL AS BIGINT) as WM_LOCATION_ID", 
# 	"CAST(NULL AS BIGINT) as PUTAWAY_ELIGIBLE_FLAG", 
# 	"CAST(NULL AS BIGINT) as MARK_FOR_DELETION_FLAG", 
# 	"CAST(NULL AS BIGINT) as WM_CREATED_SOURCE_TYPE", 
# 	"CAST(NULL AS STRING) as WM_CREATED_SOURCE", 
# 	"CAST(NULL AS TIMESTAMP) as WM_CREATED_TSTMP", 
# 	"CAST(NULL AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE", 
# 	"CAST(NULL AS STRING) as WM_LAST_UPDATED_SOURCE", 
# 	"CAST(NULL AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", 
# 	"CAST(DELETE_FLAG3 AS BIGINT) as DELETE_FLAG", 
# 	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", 
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP" 
# )
# Shortcut_to_WM_YARD_ZONE11.write.saveAsTable(f'{raw}.WM_YARD_ZONE')