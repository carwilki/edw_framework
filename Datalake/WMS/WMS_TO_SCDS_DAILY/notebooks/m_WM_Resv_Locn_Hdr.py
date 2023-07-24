#Code converted on 2023-06-22 20:25:02
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
refined_perf_table = f"{refine}.WM_RESV_LOCN_HDR"
raw_perf_table = f"{raw}.WM_RESV_LOCN_HDR_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic= ' -- ' # args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE, type SOURCE 
# COLUMN COUNT: 27

SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE = spark.sql(f"""SELECT
DC_NBR,
RESV_LOCN_HDR_ID,
LOCN_ID,
LOCN_SIZE_TYPE,
LOCN_PUTAWAY_LOCK,
INVN_LOCK_CODE,
CURR_WT,
DIRCT_WT,
MAX_WT,
CURR_VOL,
DIRCT_VOL,
MAX_VOL,
CURR_UOM_QTY,
DIRCT_UOM_QTY,
MAX_UOM_QTY,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
DEDCTN_BATCH_NBR,
DEDCTN_PACK_QTY,
PACK_ZONE,
SORT_LOCN_FLAG,
INBD_STAGING_FLAG,
WM_VERSION_ID,
DEDCTN_ITEM_ID,
LOCN_HDR_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 27

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE_temp = SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE.toDF(*["SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___" + col for col in SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___LOCN_ID as LOCN_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___INVN_LOCK_CODE as INVN_LOCK_CODE", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___CURR_WT as CURR_WT", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DIRCT_WT as DIRCT_WT", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___MAX_WT as MAX_WT", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___CURR_VOL as CURR_VOL", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DIRCT_VOL as DIRCT_VOL", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___MAX_VOL as MAX_VOL", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___CURR_UOM_QTY as CURR_UOM_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___MAX_UOM_QTY as MAX_UOM_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___USER_ID as USER_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___PACK_ZONE as PACK_ZONE", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___INBD_STAGING_FLAG as INBD_STAGING_FLAG", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___WM_VERSION_ID as WM_VERSION_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___LOCN_HDR_ID as LOCN_HDR_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_RESV_LOCN_HDR, type SOURCE 
# COLUMN COUNT: 29

SQ_Shortcut_to_WM_RESV_LOCN_HDR = spark.sql(f"""SELECT
LOCATION_ID,
WM_RESV_LOCN_HDR_ID,
WM_LOCN_ID,
WM_LOCN_HDR_ID,
WM_LOCN_PUTAWAY_LOCK,
WM_LOCN_SIZE_TYPE,
WM_INVN_LOCK_CD,
WM_PACK_ZONE,
SORT_LOCN_FLAG,
INBD_STAGING_FLAG,
WM_DEDCTN_BATCH_NBR,
WM_DEDCTN_ITEM_ID,
DEDCTN_PACK_QTY,
CURR_WT,
CURR_VOL,
CURR_UOM_QTY,
DIRCT_WT,
DIRCT_VOL,
DIRCT_UOM_QTY,
MAX_WT,
MAX_VOL,
MAX_UOM_QTY,
WM_USER_ID,
WM_VERSION_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
DELETE_FLAG,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG =0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 29

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_RESV_LOCN_HDR, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 58

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_RESV_LOCN_HDR_temp = SQ_Shortcut_to_WM_RESV_LOCN_HDR.toDF(*["SQ_Shortcut_to_WM_RESV_LOCN_HDR___" + col for col in SQ_Shortcut_to_WM_RESV_LOCN_HDR.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_RESV_LOCN_HDR = SQ_Shortcut_to_WM_RESV_LOCN_HDR_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_RESV_LOCN_HDR_temp.SQ_Shortcut_to_WM_RESV_LOCN_HDR___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_RESV_LOCN_HDR_temp.SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_RESV_LOCN_HDR_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___RESV_LOCN_HDR_ID],'fullouter').selectExpr( 
	"JNR_SITE_PROFILE___o_DC_NBR as o_DC_NBR", 
	"JNR_SITE_PROFILE___RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
	"JNR_SITE_PROFILE___LOCN_ID as LOCN_ID", 
	"JNR_SITE_PROFILE___LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
	"JNR_SITE_PROFILE___LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
	"JNR_SITE_PROFILE___INVN_LOCK_CODE as INVN_LOCK_CODE", 
	"JNR_SITE_PROFILE___CURR_WT as CURR_WT", 
	"JNR_SITE_PROFILE___DIRCT_WT as DIRCT_WT", 
	"JNR_SITE_PROFILE___MAX_WT as MAX_WT", 
	"JNR_SITE_PROFILE___CURR_VOL as CURR_VOL", 
	"JNR_SITE_PROFILE___DIRCT_VOL as DIRCT_VOL", 
	"JNR_SITE_PROFILE___MAX_VOL as MAX_VOL", 
	"JNR_SITE_PROFILE___CURR_UOM_QTY as CURR_UOM_QTY", 
	"JNR_SITE_PROFILE___DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
	"JNR_SITE_PROFILE___MAX_UOM_QTY as MAX_UOM_QTY", 
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_SITE_PROFILE___USER_ID as USER_ID", 
	"JNR_SITE_PROFILE___DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
	"JNR_SITE_PROFILE___DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
	"JNR_SITE_PROFILE___PACK_ZONE as PACK_ZONE", 
	"JNR_SITE_PROFILE___SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
	"JNR_SITE_PROFILE___INBD_STAGING_FLAG as INBD_STAGING_FLAG", 
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", 
	"JNR_SITE_PROFILE___DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
	"JNR_SITE_PROFILE___LOCN_HDR_ID as LOCN_HDR_ID", 
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___STORE_NBR as STORE_NBR", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___LOCATION_ID as in_LOCATION_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_RESV_LOCN_HDR_ID as WM_RESV_LOCN_HDR_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_LOCN_ID as WM_LOCN_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_LOCN_PUTAWAY_LOCK as WM_LOCN_PUTAWAY_LOCK", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_LOCN_SIZE_TYPE as WM_LOCN_SIZE_TYPE", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_INVN_LOCK_CD as WM_INVN_LOCK_CD", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_PACK_ZONE as WM_PACK_ZONE", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___SORT_LOCN_FLAG as in_SORT_LOCN_FLAG", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___INBD_STAGING_FLAG as in_INBD_STAGING_FLAG", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_DEDCTN_BATCH_NBR as WM_DEDCTN_BATCH_NBR", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_DEDCTN_ITEM_ID as WM_DEDCTN_ITEM_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___DEDCTN_PACK_QTY as in_DEDCTN_PACK_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___CURR_WT as in_CURR_WT", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___CURR_VOL as in_CURR_VOL", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___CURR_UOM_QTY as in_CURR_UOM_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___DIRCT_WT as in_DIRCT_WT", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___DIRCT_VOL as in_DIRCT_VOL", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___DIRCT_UOM_QTY as in_DIRCT_UOM_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___MAX_WT as in_MAX_WT", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___MAX_VOL as in_MAX_VOL", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___MAX_UOM_QTY as in_MAX_UOM_QTY", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_USER_ID as WM_USER_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_VERSION_ID as in_WM_VERSION_ID", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_CREATE_TSTMP as WM_CREATE_TSTMP", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___WM_MOD_TSTMP as WM_MOD_TSTMP", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___UPDATE_TSTMP as in_UPDATE_TSTMP", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___LOAD_TSTMP as in_LOAD_TSTMP", 
	"SQ_Shortcut_to_WM_RESV_LOCN_HDR___DELETE_FLAG as in_DELETE_FLAG")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 57

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_RESV_LOCN_HDR_temp = JNR_WM_RESV_LOCN_HDR.toDF(*["JNR_WM_RESV_LOCN_HDR___" + col for col in JNR_WM_RESV_LOCN_HDR.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_RESV_LOCN_HDR_temp.selectExpr( 
	"JNR_WM_RESV_LOCN_HDR___RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
	"JNR_WM_RESV_LOCN_HDR___LOCN_ID as LOCN_ID", 
	"JNR_WM_RESV_LOCN_HDR___LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
	"JNR_WM_RESV_LOCN_HDR___LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
	"JNR_WM_RESV_LOCN_HDR___INVN_LOCK_CODE as INVN_LOCK_CODE", 
	"JNR_WM_RESV_LOCN_HDR___CURR_WT as CURR_WT", 
	"JNR_WM_RESV_LOCN_HDR___DIRCT_WT as DIRCT_WT", 
	"JNR_WM_RESV_LOCN_HDR___MAX_WT as MAX_WT", 
	"JNR_WM_RESV_LOCN_HDR___CURR_VOL as CURR_VOL", 
	"JNR_WM_RESV_LOCN_HDR___DIRCT_VOL as DIRCT_VOL", 
	"JNR_WM_RESV_LOCN_HDR___MAX_VOL as MAX_VOL", 
	"JNR_WM_RESV_LOCN_HDR___CURR_UOM_QTY as CURR_UOM_QTY", 
	"JNR_WM_RESV_LOCN_HDR___DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
	"JNR_WM_RESV_LOCN_HDR___MAX_UOM_QTY as MAX_UOM_QTY", 
	"JNR_WM_RESV_LOCN_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_WM_RESV_LOCN_HDR___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_WM_RESV_LOCN_HDR___USER_ID as USER_ID", 
	"JNR_WM_RESV_LOCN_HDR___DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
	"JNR_WM_RESV_LOCN_HDR___DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
	"JNR_WM_RESV_LOCN_HDR___PACK_ZONE as PACK_ZONE", 
	"JNR_WM_RESV_LOCN_HDR___SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
	"JNR_WM_RESV_LOCN_HDR___INBD_STAGING_FLAG as INBD_STAGING_FLAG", 
	"JNR_WM_RESV_LOCN_HDR___WM_VERSION_ID as WM_VERSION_ID", 
	"JNR_WM_RESV_LOCN_HDR___DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
	"JNR_WM_RESV_LOCN_HDR___LOCN_HDR_ID as LOCN_HDR_ID", 
	"JNR_WM_RESV_LOCN_HDR___LOAD_TSTMP as LOAD_TSTMP", 
	"JNR_WM_RESV_LOCN_HDR___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_RESV_LOCN_HDR___STORE_NBR as STORE_NBR", 
	"JNR_WM_RESV_LOCN_HDR___in_LOCATION_ID as in_LOCATION_ID", 
	"JNR_WM_RESV_LOCN_HDR___WM_RESV_LOCN_HDR_ID as WM_RESV_LOCN_HDR_ID", 
	"JNR_WM_RESV_LOCN_HDR___WM_LOCN_ID as WM_LOCN_ID", 
	"JNR_WM_RESV_LOCN_HDR___WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", 
	"JNR_WM_RESV_LOCN_HDR___WM_LOCN_PUTAWAY_LOCK as WM_LOCN_PUTAWAY_LOCK", 
	"JNR_WM_RESV_LOCN_HDR___WM_LOCN_SIZE_TYPE as WM_LOCN_SIZE_TYPE", 
	"JNR_WM_RESV_LOCN_HDR___WM_INVN_LOCK_CD as WM_INVN_LOCK_CD", 
	"JNR_WM_RESV_LOCN_HDR___WM_PACK_ZONE as WM_PACK_ZONE", 
	"JNR_WM_RESV_LOCN_HDR___in_SORT_LOCN_FLAG as in_SORT_LOCN_FLAG", 
	"JNR_WM_RESV_LOCN_HDR___in_INBD_STAGING_FLAG as in_INBD_STAGING_FLAG", 
	"JNR_WM_RESV_LOCN_HDR___WM_DEDCTN_BATCH_NBR as WM_DEDCTN_BATCH_NBR", 
	"JNR_WM_RESV_LOCN_HDR___WM_DEDCTN_ITEM_ID as WM_DEDCTN_ITEM_ID", 
	"JNR_WM_RESV_LOCN_HDR___in_DEDCTN_PACK_QTY as in_DEDCTN_PACK_QTY", 
	"JNR_WM_RESV_LOCN_HDR___in_CURR_WT as in_CURR_WT", 
	"JNR_WM_RESV_LOCN_HDR___in_CURR_VOL as in_CURR_VOL", 
	"JNR_WM_RESV_LOCN_HDR___in_CURR_UOM_QTY as in_CURR_UOM_QTY", 
	"JNR_WM_RESV_LOCN_HDR___in_DIRCT_WT as in_DIRCT_WT", 
	"JNR_WM_RESV_LOCN_HDR___in_DIRCT_VOL as in_DIRCT_VOL", 
	"JNR_WM_RESV_LOCN_HDR___in_DIRCT_UOM_QTY as in_DIRCT_UOM_QTY", 
	"JNR_WM_RESV_LOCN_HDR___in_MAX_WT as in_MAX_WT", 
	"JNR_WM_RESV_LOCN_HDR___in_MAX_VOL as in_MAX_VOL", 
	"JNR_WM_RESV_LOCN_HDR___in_MAX_UOM_QTY as in_MAX_UOM_QTY", 
	"JNR_WM_RESV_LOCN_HDR___WM_USER_ID as WM_USER_ID", 
	"JNR_WM_RESV_LOCN_HDR___in_WM_VERSION_ID as in_WM_VERSION_ID", 
	"JNR_WM_RESV_LOCN_HDR___WM_CREATE_TSTMP as WM_CREATE_TSTMP", 
	"JNR_WM_RESV_LOCN_HDR___WM_MOD_TSTMP as WM_MOD_TSTMP", 
	"JNR_WM_RESV_LOCN_HDR___in_UPDATE_TSTMP as in_UPDATE_TSTMP", 
	"JNR_WM_RESV_LOCN_HDR___in_LOAD_TSTMP as in_LOAD_TSTMP", 
	"JNR_WM_RESV_LOCN_HDR___in_DELETE_FLAG as in_DELETE_FLAG").filter(expr("WM_RESV_LOCN_HDR_ID IS NULL OR RESV_LOCN_HDR_ID IS NULL OR (NOT WM_RESV_LOCN_HDR_ID IS NULL AND (COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(WM_CREATE_TSTMP, date'1900-01-01')) OR (COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(WM_MOD_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 61

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
    .withColumn("FIL_UNCHANGED_RECORDS___v_CREATE_DATE_TIME", expr("""IF(FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_MOD_DATE_TIME", expr("""IF(FIL_UNCHANGED_RECORDS___MOD_DATE_TIME IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___MOD_DATE_TIME)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_WM_CREATE_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___WM_CREATE_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___WM_CREATE_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_WM_MOD_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___WM_MOD_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___WM_MOD_TSTMP)"""))
    
EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___LOCN_ID as LOCN_ID", 
	"FIL_UNCHANGED_RECORDS___LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
	"FIL_UNCHANGED_RECORDS___INVN_LOCK_CODE as INVN_LOCK_CODE", 
	"FIL_UNCHANGED_RECORDS___CURR_WT as CURR_WT", 
	"FIL_UNCHANGED_RECORDS___DIRCT_WT as DIRCT_WT", 
	"FIL_UNCHANGED_RECORDS___MAX_WT as MAX_WT", 
	"FIL_UNCHANGED_RECORDS___CURR_VOL as CURR_VOL", 
	"FIL_UNCHANGED_RECORDS___DIRCT_VOL as DIRCT_VOL", 
	"FIL_UNCHANGED_RECORDS___MAX_VOL as MAX_VOL", 
	"FIL_UNCHANGED_RECORDS___CURR_UOM_QTY as CURR_UOM_QTY", 
	"FIL_UNCHANGED_RECORDS___DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
	"FIL_UNCHANGED_RECORDS___MAX_UOM_QTY as MAX_UOM_QTY", 
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", 
	"FIL_UNCHANGED_RECORDS___DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
	"FIL_UNCHANGED_RECORDS___DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
	"FIL_UNCHANGED_RECORDS___PACK_ZONE as PACK_ZONE", 
	"FIL_UNCHANGED_RECORDS___SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
	"CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___INBD_STAGING_FLAG)) IN ('Y', '1') THEN '1' ELSE '0' END as INBD_STAGING_FLAG_EXP", 
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID as WM_VERSION_ID", 
	"FIL_UNCHANGED_RECORDS___DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
	"FIL_UNCHANGED_RECORDS___LOCN_HDR_ID as LOCN_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___LOAD_TSTMP as LOAD_TSTMP", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___STORE_NBR as STORE_NBR", 
	"FIL_UNCHANGED_RECORDS___in_LOCATION_ID as in_LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___WM_RESV_LOCN_HDR_ID as WM_RESV_LOCN_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___WM_LOCN_ID as WM_LOCN_ID", 
	"FIL_UNCHANGED_RECORDS___WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", 
	"FIL_UNCHANGED_RECORDS___WM_LOCN_PUTAWAY_LOCK as WM_LOCN_PUTAWAY_LOCK", 
	"FIL_UNCHANGED_RECORDS___WM_LOCN_SIZE_TYPE as WM_LOCN_SIZE_TYPE", 
	"FIL_UNCHANGED_RECORDS___WM_INVN_LOCK_CD as WM_INVN_LOCK_CD", 
	"FIL_UNCHANGED_RECORDS___WM_PACK_ZONE as WM_PACK_ZONE", 
	"FIL_UNCHANGED_RECORDS___in_SORT_LOCN_FLAG as in_SORT_LOCN_FLAG", 
	"FIL_UNCHANGED_RECORDS___in_INBD_STAGING_FLAG as in_INBD_STAGING_FLAG", 
	"FIL_UNCHANGED_RECORDS___WM_DEDCTN_BATCH_NBR as WM_DEDCTN_BATCH_NBR", 
	"FIL_UNCHANGED_RECORDS___WM_DEDCTN_ITEM_ID as WM_DEDCTN_ITEM_ID", 
	"FIL_UNCHANGED_RECORDS___in_DEDCTN_PACK_QTY as in_DEDCTN_PACK_QTY", 
	"FIL_UNCHANGED_RECORDS___in_CURR_WT as in_CURR_WT", 
	"FIL_UNCHANGED_RECORDS___in_CURR_VOL as in_CURR_VOL", 
	"FIL_UNCHANGED_RECORDS___in_CURR_UOM_QTY as in_CURR_UOM_QTY", 
	"FIL_UNCHANGED_RECORDS___in_DIRCT_WT as in_DIRCT_WT", 
	"FIL_UNCHANGED_RECORDS___in_DIRCT_VOL as in_DIRCT_VOL", 
	"FIL_UNCHANGED_RECORDS___in_DIRCT_UOM_QTY as in_DIRCT_UOM_QTY", 
	"FIL_UNCHANGED_RECORDS___in_MAX_WT as in_MAX_WT", 
	"FIL_UNCHANGED_RECORDS___in_MAX_VOL as in_MAX_VOL", 
	"FIL_UNCHANGED_RECORDS___in_MAX_UOM_QTY as in_MAX_UOM_QTY", 
	"FIL_UNCHANGED_RECORDS___WM_USER_ID as WM_USER_ID", 
	"FIL_UNCHANGED_RECORDS___in_WM_VERSION_ID as in_WM_VERSION_ID", 
	"FIL_UNCHANGED_RECORDS___WM_CREATE_TSTMP as WM_CREATE_TSTMP", 
	"FIL_UNCHANGED_RECORDS___WM_MOD_TSTMP as WM_MOD_TSTMP", 
	"FIL_UNCHANGED_RECORDS___in_UPDATE_TSTMP as in_UPDATE_TSTMP", 
	"FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP as in_LOAD_TSTMP", 
	"FIL_UNCHANGED_RECORDS___in_DELETE_FLAG as in_DELETE_FLAG", 
	"IF(FIL_UNCHANGED_RECORDS___RESV_LOCN_HDR_ID IS NULL AND FIL_UNCHANGED_RECORDS___WM_RESV_LOCN_HDR_ID IS NOT NULL, 1, 0) as DELETE_FLAG_EXP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP_EXP", 
	"IF(FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP) as LOAD_TSTMP_EXP", 
	f"IF(FIL_UNCHANGED_RECORDS___RESV_LOCN_HDR_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___WM_RESV_LOCN_HDR_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_RECORDS___RESV_LOCN_HDR_ID IS NULL AND FIL_UNCHANGED_RECORDS___WM_RESV_LOCN_HDR_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_WM_CREATE_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_UNCHANGED_RECORDS___v_WM_MOD_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) ), 'DELETE', IF(FIL_UNCHANGED_RECORDS___RESV_LOCN_HDR_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___WM_RESV_LOCN_HDR_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_WM_CREATE_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATE_DATE_TIME OR FIL_UNCHANGED_RECORDS___v_WM_MOD_TSTMP <> FIL_UNCHANGED_RECORDS___v_MOD_DATE_TIME ), 'UPDATE', NULL))) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------
# Processing node RTR_DELETE, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 61


# Creating output dataframe for RTR_DELETE, output group DELETE
# RTR_DELETE_DELETE = EXP_UPD_VALIDATOR.selectExpr( 
# 	"EXP_UPD_VALIDATOR.RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
# 	"EXP_UPD_VALIDATOR.LOCN_ID as LOCN_ID", 
# 	"EXP_UPD_VALIDATOR.LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
# 	"EXP_UPD_VALIDATOR.LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
# 	"EXP_UPD_VALIDATOR.INVN_LOCK_CODE as INVN_LOCK_CODE", 
# 	"EXP_UPD_VALIDATOR.CURR_WT as CURR_WT", 
# 	"EXP_UPD_VALIDATOR.DIRCT_WT as DIRCT_WT", 
# 	"EXP_UPD_VALIDATOR.MAX_WT as MAX_WT", 
# 	"EXP_UPD_VALIDATOR.CURR_VOL as CURR_VOL", 
# 	"EXP_UPD_VALIDATOR.DIRCT_VOL as DIRCT_VOL", 
# 	"EXP_UPD_VALIDATOR.MAX_VOL as MAX_VOL", 
# 	"EXP_UPD_VALIDATOR.CURR_UOM_QTY as CURR_UOM_QTY", 
# 	"EXP_UPD_VALIDATOR.DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
# 	"EXP_UPD_VALIDATOR.MAX_UOM_QTY as MAX_UOM_QTY", 
# 	"EXP_UPD_VALIDATOR.CREATE_DATE_TIME as CREATE_DATE_TIME", 
# 	"EXP_UPD_VALIDATOR.MOD_DATE_TIME as MOD_DATE_TIME", 
# 	"EXP_UPD_VALIDATOR.USER_ID as USER_ID", 
# 	"EXP_UPD_VALIDATOR.DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
# 	"EXP_UPD_VALIDATOR.DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
# 	"EXP_UPD_VALIDATOR.PACK_ZONE as PACK_ZONE", 
# 	"EXP_UPD_VALIDATOR.SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
# 	"EXP_UPD_VALIDATOR.INBD_STAGING_FLAG_EXP as INBD_STAGING_FLAG", 
# 	"EXP_UPD_VALIDATOR.WM_VERSION_ID as WM_VERSION_ID", 
# 	"EXP_UPD_VALIDATOR.DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
# 	"EXP_UPD_VALIDATOR.LOCN_HDR_ID as LOCN_HDR_ID", 
# 	"EXP_UPD_VALIDATOR.LOAD_TSTMP as LOAD_TSTMP", 
# 	"EXP_UPD_VALIDATOR.LOCATION_ID as LOCATION_ID", 
# 	"EXP_UPD_VALIDATOR.STORE_NBR as STORE_NBR", 
# 	"EXP_UPD_VALIDATOR.in_LOCATION_ID as in_LOCATION_ID", 
# 	"EXP_UPD_VALIDATOR.WM_RESV_LOCN_HDR_ID as WM_RESV_LOCN_HDR_ID", 
# 	"EXP_UPD_VALIDATOR.WM_LOCN_ID as WM_LOCN_ID", 
# 	"EXP_UPD_VALIDATOR.WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", 
# 	"EXP_UPD_VALIDATOR.WM_LOCN_PUTAWAY_LOCK as WM_LOCN_PUTAWAY_LOCK", 
# 	"EXP_UPD_VALIDATOR.WM_LOCN_SIZE_TYPE as WM_LOCN_SIZE_TYPE", 
# 	"EXP_UPD_VALIDATOR.WM_INVN_LOCK_CD as WM_INVN_LOCK_CD", 
# 	"EXP_UPD_VALIDATOR.WM_PACK_ZONE as WM_PACK_ZONE", 
# 	"EXP_UPD_VALIDATOR.in_SORT_LOCN_FLAG as in_SORT_LOCN_FLAG", 
# 	"EXP_UPD_VALIDATOR.in_INBD_STAGING_FLAG as in_INBD_STAGING_FLAG", 
# 	"EXP_UPD_VALIDATOR.WM_DEDCTN_BATCH_NBR as WM_DEDCTN_BATCH_NBR", 
# 	"EXP_UPD_VALIDATOR.WM_DEDCTN_ITEM_ID as WM_DEDCTN_ITEM_ID", 
# 	"EXP_UPD_VALIDATOR.in_DEDCTN_PACK_QTY as in_DEDCTN_PACK_QTY", 
# 	"EXP_UPD_VALIDATOR.in_CURR_WT as in_CURR_WT", 
# 	"EXP_UPD_VALIDATOR.in_CURR_VOL as in_CURR_VOL", 
# 	"EXP_UPD_VALIDATOR.in_CURR_UOM_QTY as in_CURR_UOM_QTY", 
# 	"EXP_UPD_VALIDATOR.in_DIRCT_WT as in_DIRCT_WT", 
# 	"EXP_UPD_VALIDATOR.in_DIRCT_VOL as in_DIRCT_VOL", 
# 	"EXP_UPD_VALIDATOR.in_DIRCT_UOM_QTY as in_DIRCT_UOM_QTY", 
# 	"EXP_UPD_VALIDATOR.in_MAX_WT as in_MAX_WT", 
# 	"EXP_UPD_VALIDATOR.in_MAX_VOL as in_MAX_VOL", 
# 	"EXP_UPD_VALIDATOR.in_MAX_UOM_QTY as in_MAX_UOM_QTY", 
# 	"EXP_UPD_VALIDATOR.WM_USER_ID as WM_USER_ID", 
# 	"EXP_UPD_VALIDATOR.in_WM_VERSION_ID as in_WM_VERSION_ID", 
# 	"EXP_UPD_VALIDATOR.WM_CREATE_TSTMP as WM_CREATE_TSTMP", 
# 	"EXP_UPD_VALIDATOR.WM_MOD_TSTMP as WM_MOD_TSTMP", 
# 	"EXP_UPD_VALIDATOR.in_UPDATE_TSTMP as in_UPDATE_TSTMP", 
# 	"EXP_UPD_VALIDATOR.in_LOAD_TSTMP as in_LOAD_TSTMP", 
# 	"EXP_UPD_VALIDATOR.DELETE_FLAG_EXP as DELETE_FLAG_EXP", 
# 	"EXP_UPD_VALIDATOR.UPDATE_TSTMP_EXP as UPDATE_TSTMP_EXP", 
# 	"EXP_UPD_VALIDATOR.LOAD_TSTMP_EXP as LOAD_TSTMP_EXP", 
# 	"EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR", 
# 	"EXP_UPD_VALIDATOR.in_DELETE_FLAG as in_DELETE_FLAG"
# ).select(col('sys_row_id'), 
# 	col('RESV_LOCN_HDR_ID').alias('RESV_LOCN_HDR_ID3'), 
# 	col('LOCN_ID').alias('LOCN_ID3'), 
# 	col('LOCN_SIZE_TYPE').alias('LOCN_SIZE_TYPE3'), 
# 	col('LOCN_PUTAWAY_LOCK').alias('LOCN_PUTAWAY_LOCK3'), 
# 	col('INVN_LOCK_CODE').alias('INVN_LOCK_CODE3'), 
# 	col('CURR_WT').alias('CURR_WT3'), 
# 	col('DIRCT_WT').alias('DIRCT_WT3'), 
# 	col('MAX_WT').alias('MAX_WT3'), 
# 	col('CURR_VOL').alias('CURR_VOL3'), 
# 	col('DIRCT_VOL').alias('DIRCT_VOL3'), 
# 	col('MAX_VOL').alias('MAX_VOL3'), 
# 	col('CURR_UOM_QTY').alias('CURR_UOM_QTY3'), 
# 	col('DIRCT_UOM_QTY').alias('DIRCT_UOM_QTY3'), 
# 	col('MAX_UOM_QTY').alias('MAX_UOM_QTY3'), 
# 	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME3'), 
# 	col('MOD_DATE_TIME').alias('MOD_DATE_TIME3'), 
# 	col('USER_ID').alias('USER_ID3'), 
# 	col('DEDCTN_BATCH_NBR').alias('DEDCTN_BATCH_NBR3'), 
# 	col('DEDCTN_PACK_QTY').alias('DEDCTN_PACK_QTY3'), 
# 	col('PACK_ZONE').alias('PACK_ZONE3'), 
# 	col('SORT_LOCN_FLAG').alias('SORT_LOCN_FLAG3'), 
# 	col('INBD_STAGING_FLAG').alias('INBD_STAGING_FLAG3'), 
# 	col('WM_VERSION_ID').alias('WM_VERSION_ID3'), 
# 	col('DEDCTN_ITEM_ID').alias('DEDCTN_ITEM_ID3'), 
# 	col('LOCN_HDR_ID').alias('LOCN_HDR_ID3'), 
# 	col('LOAD_TSTMP').alias('LOAD_TSTMP3'), 
# 	col('LOCATION_ID').alias('LOCATION_ID3'), 
# 	col('STORE_NBR').alias('STORE_NBR3'), 
# 	col('in_LOCATION_ID').alias('in_LOCATION_ID3'), 
# 	col('WM_RESV_LOCN_HDR_ID').alias('WM_RESV_LOCN_HDR_ID3'), 
# 	col('WM_LOCN_ID').alias('WM_LOCN_ID3'), 
# 	col('WM_LOCN_HDR_ID').alias('WM_LOCN_HDR_ID3'), 
# 	col('WM_LOCN_PUTAWAY_LOCK').alias('WM_LOCN_PUTAWAY_LOCK3'), 
# 	col('WM_LOCN_SIZE_TYPE').alias('WM_LOCN_SIZE_TYPE3'), 
# 	col('WM_INVN_LOCK_CD').alias('WM_INVN_LOCK_CD3'), 
# 	col('WM_PACK_ZONE').alias('WM_PACK_ZONE3'), 
# 	col('in_SORT_LOCN_FLAG').alias('in_SORT_LOCN_FLAG3'), 
# 	col('in_INBD_STAGING_FLAG').alias('in_INBD_STAGING_FLAG3'), 
# 	col('WM_DEDCTN_BATCH_NBR').alias('WM_DEDCTN_BATCH_NBR3'), 
# 	col('WM_DEDCTN_ITEM_ID').alias('WM_DEDCTN_ITEM_ID3'), 
# 	col('in_DEDCTN_PACK_QTY').alias('in_DEDCTN_PACK_QTY3'), 
# 	col('in_CURR_WT').alias('in_CURR_WT3'), 
# 	col('in_CURR_VOL').alias('in_CURR_VOL3'), 
# 	col('in_CURR_UOM_QTY').alias('in_CURR_UOM_QTY3'), 
# 	col('in_DIRCT_WT').alias('in_DIRCT_WT3'), 
# 	col('in_DIRCT_VOL').alias('in_DIRCT_VOL3'), 
# 	col('in_DIRCT_UOM_QTY').alias('in_DIRCT_UOM_QTY3'), 
# 	col('in_MAX_WT').alias('in_MAX_WT3'), 
# 	col('in_MAX_VOL').alias('in_MAX_VOL3'), 
# 	col('in_MAX_UOM_QTY').alias('in_MAX_UOM_QTY3'), 
# 	col('WM_USER_ID').alias('WM_USER_ID3'), 
# 	col('in_WM_VERSION_ID').alias('in_WM_VERSION_ID3'), 
# 	col('WM_CREATE_TSTMP').alias('WM_CREATE_TSTMP3'), 
# 	col('WM_MOD_TSTMP').alias('WM_MOD_TSTMP3'), 
# 	col('in_UPDATE_TSTMP').alias('in_UPDATE_TSTMP3'), 
# 	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP3'), 
# 	col('DELETE_FLAG_EXP').alias('DELETE_FLAG_EXP3'), 
# 	col('UPDATE_TSTMP_EXP').alias('UPDATE_TSTMP_EXP3'), 
# 	col('LOAD_TSTMP_EXP').alias('LOAD_TSTMP_EXP3'), 
# 	col('o_UPDATE_VALIDATOR').alias('o_UPDATE_VALIDATOR3'), 
# 	col('in_DELETE_FLAG').alias('in_DELETE_FLAG3')).filter("o_UPDATE_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_DELETE, output group INSERT_UPDATE
RTR_DELETE_INSERT_UPDATE = EXP_UPD_VALIDATOR.selectExpr( 
	"RESV_LOCN_HDR_ID as RESV_LOCN_HDR_ID", 
	"LOCN_ID as LOCN_ID", 
	"LOCN_SIZE_TYPE as LOCN_SIZE_TYPE", 
	"LOCN_PUTAWAY_LOCK as LOCN_PUTAWAY_LOCK", 
	"INVN_LOCK_CODE as INVN_LOCK_CODE", 
	"CURR_WT as CURR_WT", 
	"DIRCT_WT as DIRCT_WT", 
	"MAX_WT as MAX_WT", 
	"CURR_VOL as CURR_VOL", 
	"DIRCT_VOL as DIRCT_VOL", 
	"MAX_VOL as MAX_VOL", 
	"CURR_UOM_QTY as CURR_UOM_QTY", 
	"DIRCT_UOM_QTY as DIRCT_UOM_QTY", 
	"MAX_UOM_QTY as MAX_UOM_QTY", 
	"CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"MOD_DATE_TIME as MOD_DATE_TIME", 
	"USER_ID as USER_ID", 
	"DEDCTN_BATCH_NBR as DEDCTN_BATCH_NBR", 
	"DEDCTN_PACK_QTY as DEDCTN_PACK_QTY", 
	"PACK_ZONE as PACK_ZONE", 
	"SORT_LOCN_FLAG as SORT_LOCN_FLAG", 
	"INBD_STAGING_FLAG_EXP as INBD_STAGING_FLAG", 
	"WM_VERSION_ID as WM_VERSION_ID", 
	"DEDCTN_ITEM_ID as DEDCTN_ITEM_ID", 
	"LOCN_HDR_ID as LOCN_HDR_ID", 
	"LOAD_TSTMP as LOAD_TSTMP", 
	"LOCATION_ID as LOCATION_ID", 
	"STORE_NBR as STORE_NBR", 
	"in_LOCATION_ID as in_LOCATION_ID", 
	"WM_RESV_LOCN_HDR_ID as WM_RESV_LOCN_HDR_ID", 
	"WM_LOCN_ID as WM_LOCN_ID", 
	"WM_LOCN_HDR_ID as WM_LOCN_HDR_ID", 
	"WM_LOCN_PUTAWAY_LOCK as WM_LOCN_PUTAWAY_LOCK", 
	"WM_LOCN_SIZE_TYPE as WM_LOCN_SIZE_TYPE", 
	"WM_INVN_LOCK_CD as WM_INVN_LOCK_CD", 
	"WM_PACK_ZONE as WM_PACK_ZONE", 
	"in_SORT_LOCN_FLAG as in_SORT_LOCN_FLAG", 
	"in_INBD_STAGING_FLAG as in_INBD_STAGING_FLAG", 
	"WM_DEDCTN_BATCH_NBR as WM_DEDCTN_BATCH_NBR", 
	"WM_DEDCTN_ITEM_ID as WM_DEDCTN_ITEM_ID", 
	"in_DEDCTN_PACK_QTY as in_DEDCTN_PACK_QTY", 
	"in_CURR_WT as in_CURR_WT", 
	"in_CURR_VOL as in_CURR_VOL", 
	"in_CURR_UOM_QTY as in_CURR_UOM_QTY", 
	"in_DIRCT_WT as in_DIRCT_WT", 
	"in_DIRCT_VOL as in_DIRCT_VOL", 
	"in_DIRCT_UOM_QTY as in_DIRCT_UOM_QTY", 
	"in_MAX_WT as in_MAX_WT", 
	"in_MAX_VOL as in_MAX_VOL", 
	"in_MAX_UOM_QTY as in_MAX_UOM_QTY", 
	"WM_USER_ID as WM_USER_ID", 
	"in_WM_VERSION_ID as in_WM_VERSION_ID", 
	"WM_CREATE_TSTMP as WM_CREATE_TSTMP", 
	"WM_MOD_TSTMP as WM_MOD_TSTMP", 
	"in_UPDATE_TSTMP as in_UPDATE_TSTMP", 
	"in_LOAD_TSTMP as in_LOAD_TSTMP", 
	"DELETE_FLAG_EXP as DELETE_FLAG_EXP", 
	"UPDATE_TSTMP_EXP as UPDATE_TSTMP_EXP", 
	"LOAD_TSTMP_EXP as LOAD_TSTMP_EXP", 
	"o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR", 
	"in_DELETE_FLAG as in_DELETE_FLAG" ,
   "sys_row_id as sys_row_id"
).select(col('sys_row_id'), 
	col('RESV_LOCN_HDR_ID').alias('RESV_LOCN_HDR_ID1'), 
	col('LOCN_ID').alias('LOCN_ID1'), 
	col('LOCN_SIZE_TYPE').alias('LOCN_SIZE_TYPE1'), 
	col('LOCN_PUTAWAY_LOCK').alias('LOCN_PUTAWAY_LOCK1'), 
	col('INVN_LOCK_CODE').alias('INVN_LOCK_CODE1'), 
	col('CURR_WT').alias('CURR_WT1'), 
	col('DIRCT_WT').alias('DIRCT_WT1'), 
	col('MAX_WT').alias('MAX_WT1'), 
	col('CURR_VOL').alias('CURR_VOL1'), 
	col('DIRCT_VOL').alias('DIRCT_VOL1'), 
	col('MAX_VOL').alias('MAX_VOL1'), 
	col('CURR_UOM_QTY').alias('CURR_UOM_QTY1'), 
	col('DIRCT_UOM_QTY').alias('DIRCT_UOM_QTY1'), 
	col('MAX_UOM_QTY').alias('MAX_UOM_QTY1'), 
	col('CREATE_DATE_TIME').alias('CREATE_DATE_TIME1'), 
	col('MOD_DATE_TIME').alias('MOD_DATE_TIME1'), 
	col('USER_ID').alias('USER_ID1'), 
	col('DEDCTN_BATCH_NBR').alias('DEDCTN_BATCH_NBR1'), 
	col('DEDCTN_PACK_QTY').alias('DEDCTN_PACK_QTY1'), 
	col('PACK_ZONE').alias('PACK_ZONE1'), 
	col('SORT_LOCN_FLAG').alias('SORT_LOCN_FLAG1'), 
	col('INBD_STAGING_FLAG').alias('INBD_STAGING_FLAG1'), 
	col('WM_VERSION_ID').alias('WM_VERSION_ID1'), 
	col('DEDCTN_ITEM_ID').alias('DEDCTN_ITEM_ID1'), 
	col('LOCN_HDR_ID').alias('LOCN_HDR_ID1'), 
	col('LOAD_TSTMP').alias('LOAD_TSTMP1'), 
	col('LOCATION_ID').alias('LOCATION_ID1'), 
	col('STORE_NBR').alias('STORE_NBR1'), 
	col('in_LOCATION_ID').alias('in_LOCATION_ID1'), 
	col('WM_RESV_LOCN_HDR_ID').alias('WM_RESV_LOCN_HDR_ID1'), 
	col('WM_LOCN_ID').alias('WM_LOCN_ID1'), 
	col('WM_LOCN_HDR_ID').alias('WM_LOCN_HDR_ID1'), 
	col('WM_LOCN_PUTAWAY_LOCK').alias('WM_LOCN_PUTAWAY_LOCK1'), 
	col('WM_LOCN_SIZE_TYPE').alias('WM_LOCN_SIZE_TYPE1'), 
	col('WM_INVN_LOCK_CD').alias('WM_INVN_LOCK_CD1'), 
	col('WM_PACK_ZONE').alias('WM_PACK_ZONE1'), 
	col('in_SORT_LOCN_FLAG').alias('in_SORT_LOCN_FLAG1'), 
	col('in_INBD_STAGING_FLAG').alias('in_INBD_STAGING_FLAG1'), 
	col('WM_DEDCTN_BATCH_NBR').alias('WM_DEDCTN_BATCH_NBR1'), 
	col('WM_DEDCTN_ITEM_ID').alias('WM_DEDCTN_ITEM_ID1'), 
	col('in_DEDCTN_PACK_QTY').alias('in_DEDCTN_PACK_QTY1'), 
	col('in_CURR_WT').alias('in_CURR_WT1'), 
	col('in_CURR_VOL').alias('in_CURR_VOL1'), 
	col('in_CURR_UOM_QTY').alias('in_CURR_UOM_QTY1'), 
	col('in_DIRCT_WT').alias('in_DIRCT_WT1'), 
	col('in_DIRCT_VOL').alias('in_DIRCT_VOL1'), 
	col('in_DIRCT_UOM_QTY').alias('in_DIRCT_UOM_QTY1'), 
	col('in_MAX_WT').alias('in_MAX_WT1'), 
	col('in_MAX_VOL').alias('in_MAX_VOL1'), 
	col('in_MAX_UOM_QTY').alias('in_MAX_UOM_QTY1'), 
	col('WM_USER_ID').alias('WM_USER_ID1'), 
	col('in_WM_VERSION_ID').alias('in_WM_VERSION_ID1'), 
	col('WM_CREATE_TSTMP').alias('WM_CREATE_TSTMP1'), 
	col('WM_MOD_TSTMP').alias('WM_MOD_TSTMP1'), 
	col('in_UPDATE_TSTMP').alias('in_UPDATE_TSTMP1'), 
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP1'), 
	col('DELETE_FLAG_EXP').alias('DELETE_FLAG_EXP1'), 
	col('UPDATE_TSTMP_EXP').alias('UPDATE_TSTMP_EXP1'), 
	col('LOAD_TSTMP_EXP').alias('LOAD_TSTMP_EXP1'), 
	col('o_UPDATE_VALIDATOR').alias('o_UPDATE_VALIDATOR1'), 
	col('in_DELETE_FLAG').alias('in_DELETE_FLAG1')).filter("o_UPDATE_VALIDATOR = 'INSERT' OR o_UPDATE_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
# RTR_DELETE_DELETE_temp = RTR_DELETE_DELETE.toDF(*["RTR_DELETE_DELETE___" + col for col in RTR_DELETE_DELETE.columns])

# UPD_DELETE = RTR_DELETE_DELETE_temp.selectExpr( 
# 	"RTR_DELETE_DELETE___in_LOCATION_ID3 as in_LOCATION_ID3", 
# 	"RTR_DELETE_DELETE___WM_RESV_LOCN_HDR_ID3 as WM_RESV_LOCN_HDR_ID3", 
# 	"RTR_DELETE_DELETE___UPDATE_TSTMP_EXP3 as UPDATE_TSTMP_EXP3", 
# 	"RTR_DELETE_DELETE___DELETE_FLAG_EXP3 as DELETE_FLAG_EXP3"
# ).withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_RESV_LOCN_HDR2, type TARGET 
# COLUMN COUNT: 29


# Shortcut_to_WM_RESV_LOCN_HDR2 = UPD_DELETE.selectExpr( 
# 	"CAST(in_LOCATION_ID3 AS BIGINT) as LOCATION_ID", 
# 	"CAST(WM_RESV_LOCN_HDR_ID3 AS BIGINT) as WM_RESV_LOCN_HDR_ID", 
# 	"CAST(NULL AS STRING) as WM_LOCN_ID", 
# 	"CAST(NULL AS BIGINT) as WM_LOCN_HDR_ID", 
# 	"CAST(NULL AS STRING) as WM_LOCN_PUTAWAY_LOCK", 
# 	"CAST(NULL AS STRING) as WM_LOCN_SIZE_TYPE", 
# 	"CAST(NULL AS STRING) as WM_INVN_LOCK_CD", 
# 	"CAST(NULL AS STRING) as WM_PACK_ZONE", 
# 	"CAST(NULL AS BIGINT) as SORT_LOCN_FLAG", 
# 	"CAST(NULL AS BIGINT) as INBD_STAGING_FLAG", 
# 	"CAST(NULL AS STRING) as WM_DEDCTN_BATCH_NBR", 
# 	"CAST(NULL AS BIGINT) as WM_DEDCTN_ITEM_ID", 
# 	"CAST(NULL AS BIGINT) as DEDCTN_PACK_QTY", 
# 	"CAST(NULL AS BIGINT) as CURR_WT", 
# 	"CAST(NULL AS BIGINT) as CURR_VOL", 
# 	"CAST(NULL AS BIGINT) as CURR_UOM_QTY", 
# 	"CAST(NULL AS BIGINT) as DIRCT_WT", 
# 	"CAST(NULL AS BIGINT) as DIRCT_VOL", 
# 	"CAST(NULL AS BIGINT) as DIRCT_UOM_QTY", 
# 	"CAST(NULL AS BIGINT) as MAX_WT", 
# 	"CAST(NULL AS BIGINT) as MAX_VOL", 
# 	"CAST(NULL AS BIGINT) as MAX_UOM_QTY", 
# 	"CAST(NULL AS STRING) as WM_USER_ID", 
# 	"CAST(NULL AS BIGINT) as WM_VERSION_ID", 
# 	"CAST(NULL AS TIMESTAMP) as WM_CREATE_TSTMP", 
# 	"CAST(NULL AS TIMESTAMP) as WM_MOD_TSTMP", 
# 	"CAST(DELETE_FLAG_EXP3 AS BIGINT) as DELETE_FLAG", 
# 	"CAST(UPDATE_TSTMP_EXP3 AS TIMESTAMP) as UPDATE_TSTMP", 
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP" 
# )
# Shortcut_to_WM_RESV_LOCN_HDR2.write.saveAsTable(f'{raw}.WM_RESV_LOCN_HDR')

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_INSERT_UPDATE_temp = RTR_DELETE_INSERT_UPDATE.toDF(*["RTR_DELETE_INSERT_UPDATE___" + col for col in RTR_DELETE_INSERT_UPDATE.columns])

UPD_INSERT_UPDATE = RTR_DELETE_INSERT_UPDATE_temp.selectExpr( 
	"RTR_DELETE_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID1", 
	"RTR_DELETE_INSERT_UPDATE___RESV_LOCN_HDR_ID1 as RESV_LOCN_HDR_ID1", 
	"RTR_DELETE_INSERT_UPDATE___LOCN_ID1 as LOCN_ID1", 
	"RTR_DELETE_INSERT_UPDATE___LOCN_SIZE_TYPE1 as LOCN_SIZE_TYPE1", 
	"RTR_DELETE_INSERT_UPDATE___LOCN_PUTAWAY_LOCK1 as LOCN_PUTAWAY_LOCK1", 
	"RTR_DELETE_INSERT_UPDATE___INVN_LOCK_CODE1 as INVN_LOCK_CODE1", 
	"RTR_DELETE_INSERT_UPDATE___CURR_WT1 as CURR_WT1", 
	"RTR_DELETE_INSERT_UPDATE___DIRCT_WT1 as DIRCT_WT1", 
	"RTR_DELETE_INSERT_UPDATE___MAX_WT1 as MAX_WT1", 
	"RTR_DELETE_INSERT_UPDATE___CURR_VOL1 as CURR_VOL1", 
	"RTR_DELETE_INSERT_UPDATE___DIRCT_VOL1 as DIRCT_VOL1", 
	"RTR_DELETE_INSERT_UPDATE___MAX_VOL1 as MAX_VOL1", 
	"RTR_DELETE_INSERT_UPDATE___CURR_UOM_QTY1 as CURR_UOM_QTY1", 
	"RTR_DELETE_INSERT_UPDATE___DIRCT_UOM_QTY1 as DIRCT_UOM_QTY1", 
	"RTR_DELETE_INSERT_UPDATE___MAX_UOM_QTY1 as MAX_UOM_QTY1", 
	"RTR_DELETE_INSERT_UPDATE___CREATE_DATE_TIME1 as CREATE_DATE_TIME1", 
	"RTR_DELETE_INSERT_UPDATE___MOD_DATE_TIME1 as MOD_DATE_TIME1", 
	"RTR_DELETE_INSERT_UPDATE___USER_ID1 as USER_ID1", 
	"RTR_DELETE_INSERT_UPDATE___DEDCTN_BATCH_NBR1 as DEDCTN_BATCH_NBR1", 
	"RTR_DELETE_INSERT_UPDATE___DEDCTN_PACK_QTY1 as DEDCTN_PACK_QTY1", 
	"RTR_DELETE_INSERT_UPDATE___PACK_ZONE1 as PACK_ZONE1", 
	"RTR_DELETE_INSERT_UPDATE___SORT_LOCN_FLAG1 as SORT_LOCN_FLAG1", 
	"RTR_DELETE_INSERT_UPDATE___INBD_STAGING_FLAG1 as INBD_STAGING_FLAG1", 
	"RTR_DELETE_INSERT_UPDATE___WM_VERSION_ID1 as WM_VERSION_ID1", 
	"RTR_DELETE_INSERT_UPDATE___DEDCTN_ITEM_ID1 as DEDCTN_ITEM_ID1", 
	"RTR_DELETE_INSERT_UPDATE___LOCN_HDR_ID1 as LOCN_HDR_ID1", 
	"RTR_DELETE_INSERT_UPDATE___UPDATE_TSTMP_EXP1 as UPDATE_TSTMP_EXP1", 
	"RTR_DELETE_INSERT_UPDATE___LOAD_TSTMP_EXP1 as LOAD_TSTMP_EXP1", 
	"RTR_DELETE_INSERT_UPDATE___o_UPDATE_VALIDATOR1 as o_UPDATE_VALIDATOR1", 
	"RTR_DELETE_INSERT_UPDATE___DELETE_FLAG_EXP1 as DELETE_FLAG_EXP1"
).withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR1') ==(lit('INSERT')),lit(0)).when(col('o_UPDATE_VALIDATOR1') ==(lit('UPDATE')),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_RESV_LOCN_HDR, type TARGET 
# COLUMN COUNT: 29


Shortcut_to_WM_RESV_LOCN_HDR = UPD_INSERT_UPDATE.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(RESV_LOCN_HDR_ID1 AS INT) as WM_RESV_LOCN_HDR_ID",
	"CAST(LOCN_ID1 AS STRING) as WM_LOCN_ID",
	"CAST(LOCN_HDR_ID1 AS INT) as WM_LOCN_HDR_ID",
	"CAST(LOCN_PUTAWAY_LOCK1 AS STRING) as WM_LOCN_PUTAWAY_LOCK",
	"CAST(LOCN_SIZE_TYPE1 AS STRING) as WM_LOCN_SIZE_TYPE",
	"CAST(INVN_LOCK_CODE1 AS STRING) as WM_INVN_LOCK_CD",
	"CAST(PACK_ZONE1 AS STRING) as WM_PACK_ZONE",
	"CAST(SORT_LOCN_FLAG1 AS TINYINT) as SORT_LOCN_FLAG",
	"CAST(INBD_STAGING_FLAG1 AS TINYINT) as INBD_STAGING_FLAG",
	"CAST(DEDCTN_BATCH_NBR1 AS STRING) as WM_DEDCTN_BATCH_NBR",
	"CAST(DEDCTN_ITEM_ID1 AS BIGINT) as WM_DEDCTN_ITEM_ID",
	"CAST(DEDCTN_PACK_QTY1 AS DECIMAL(9,2)) as DEDCTN_PACK_QTY",
	"CAST(CURR_WT1 AS DECIMAL(13,4)) as CURR_WT",
	"CAST(CURR_VOL1 AS DECIMAL(13,4)) as CURR_VOL",
	"CAST(CURR_UOM_QTY1 AS DECIMAL(9,2)) as CURR_UOM_QTY",
	"CAST(DIRCT_WT1 AS DECIMAL(13,4)) as DIRCT_WT",
	"CAST(DIRCT_VOL1 AS DECIMAL(13,4)) as DIRCT_VOL",
	"CAST(DIRCT_UOM_QTY1 AS DECIMAL(9,2)) as DIRCT_UOM_QTY",
	"CAST(MAX_WT1 AS DECIMAL(13,4)) as MAX_WT",
	"CAST(MAX_VOL1 AS DECIMAL(13,4)) as MAX_VOL",
	"CAST(MAX_UOM_QTY1 AS DECIMAL(15,5)) as MAX_UOM_QTY",
	"CAST(USER_ID1 AS STRING) as WM_USER_ID",
	"CAST(WM_VERSION_ID1 AS INT) as WM_VERSION_ID",
	"CAST(CREATE_DATE_TIME1 AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME1 AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(DELETE_FLAG_EXP1 AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP_EXP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_EXP1 AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_RESV_LOCN_HDR_ID = target.WM_RESV_LOCN_HDR_ID"""
  # refined_perf_table = "WM_RESV_LOCN_HDR"
  executeMerge(Shortcut_to_WM_RESV_LOCN_HDR, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_RESV_LOCN_HDR", "WM_RESV_LOCN_HDR", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_RESV_LOCN_HDR", "WM_RESV_LOCN_HDR","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	