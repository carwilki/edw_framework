#Code converted on 2023-06-26 10:00:56
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
raw_perf_table = f"{raw}.WM_E_AUD_LOG_PRE"
refined_perf_table = f"{refine}.WM_E_AUD_LOG"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table, refine,raw)
Del_Logic=args.Del_Logic
Soft_Delete_Logic=args.Soft_Delete_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_AUD_LOG, type SOURCE 
# COLUMN COUNT: 46

SQ_Shortcut_to_WM_E_AUD_LOG = spark.sql(f"""SELECT
LOCATION_ID,
WM_AUD_ID,
WM_WHSE,
WM_TRAN_NBR,
WM_SKU_ID,
WM_SKU_HNDL_ATTR,
WM_CRITERIA,
WM_SLOT_ATTR,
WM_ACT_ID,
WM_LPN,
WM_COMPONENT_BK,
WM_ELM_ID,
WM_FACTOR,
WM_THRUPUT_MSRMNT,
WM_MODULE_TYPE,
WM_TA_MULTIPLIER,
WM_ELS_TRAN_ID,
WM_CRIT_VAL,
WM_CRIT_TIME,
WM_CURR_SLOT,
WM_NEXT_SLOT,
WM_CURR_LOCN_CLASS,
WM_NEXT_LOCN_CLASS,
PFD_TIME,
FACTOR_TIME,
DISTANCE,
HEIGHT,
TRVL_DIR,
ELEM_DESC,
ELEM_TIME,
UOM,
TOT_TIME,
TOT_UNITS,
UNITS_PER_GRAB,
UNIT_PICK_TIME_ALLOW,
SLOT_TYPE_TIME_ALLOW,
ADDTL_TIME_ALLOW,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
WM_VERSION_ID,
WM_CREATE_TSTMP,
DELETE_FLAG,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_E_AUD_LOG_PRE, type SOURCE 
# COLUMN COUNT: 43

SQ_Shortcut_to_WM_E_AUD_LOG_PRE = spark.sql(f"""SELECT
DC_NBR,
AUD_ID,
WHSE,
TRAN_NBR,
SKU_ID,
SKU_HNDL_ATTR,
CRITERIA,
SLOT_ATTR,
FACTOR_TIME,
CURR_SLOT,
NEXT_SLOT,
DISTANCE,
HEIGHT,
TRVL_DIR,
ELEM_TIME,
UOM,
CURR_LOCN_CLASS,
NEXT_LOCN_CLASS,
TOT_TIME,
TOT_UNITS,
UNITS_PER_GRAB,
LPN,
ELEM_DESC,
CREATE_DATE_TIME,
TA_MULTIPLIER,
ELS_TRAN_ID,
CRIT_VAL,
CRIT_TIME,
ELM_ID,
FACTOR,
THRUPUT_MSRMNT,
MODULE_TYPE,
MISC_TXT_1,
MISC_TXT_2,
MISC_NUM_1,
MISC_NUM_2,
ADDTL_TIME_ALLOW,
SLOT_TYPE_TIME_ALLOW,
UNIT_PICK_TIME_ALLOW,
PFD_TIME,
VERSION_ID,
ACT_ID,
COMPONENT_BK
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 43

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_E_AUD_LOG_PRE_temp = SQ_Shortcut_to_WM_E_AUD_LOG_PRE.toDF(*["SQ_Shortcut_to_WM_E_AUD_LOG_PRE___" + col for col in SQ_Shortcut_to_WM_E_AUD_LOG_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_E_AUD_LOG_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_E_AUD_LOG_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___AUD_ID as AUD_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___TRAN_NBR as TRAN_NBR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___SKU_ID as SKU_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___SKU_HNDL_ATTR as SKU_HNDL_ATTR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___CRITERIA as CRITERIA", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___SLOT_ATTR as SLOT_ATTR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___FACTOR_TIME as FACTOR_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___CURR_SLOT as CURR_SLOT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___NEXT_SLOT as NEXT_SLOT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___DISTANCE as DISTANCE", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___HEIGHT as HEIGHT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___TRVL_DIR as TRVL_DIR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___ELEM_TIME as ELEM_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___UOM as UOM", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___CURR_LOCN_CLASS as CURR_LOCN_CLASS", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___NEXT_LOCN_CLASS as NEXT_LOCN_CLASS", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___TOT_TIME as TOT_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___TOT_UNITS as TOT_UNITS", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___UNITS_PER_GRAB as UNITS_PER_GRAB", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___LPN as LPN", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___ELEM_DESC as ELEM_DESC", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___TA_MULTIPLIER as TA_MULTIPLIER", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___ELS_TRAN_ID as ELS_TRAN_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___CRIT_VAL as CRIT_VAL", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___CRIT_TIME as CRIT_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___ELM_ID as ELM_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___FACTOR as FACTOR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___MODULE_TYPE as MODULE_TYPE", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___MISC_TXT_1 as MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___MISC_TXT_2 as MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___MISC_NUM_1 as MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___MISC_NUM_2 as MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___ADDTL_TIME_ALLOW as ADDTL_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___SLOT_TYPE_TIME_ALLOW as SLOT_TYPE_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___UNIT_PICK_TIME_ALLOW as UNIT_PICK_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___PFD_TIME as PFD_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___VERSION_ID as VERSION_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___ACT_ID as ACT_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG_PRE___COMPONENT_BK as COMPONENT_BK" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 45

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_E_AUD_LOG, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 89

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_E_AUD_LOG_temp = SQ_Shortcut_to_WM_E_AUD_LOG.toDF(*["SQ_Shortcut_to_WM_E_AUD_LOG___" + col for col in SQ_Shortcut_to_WM_E_AUD_LOG.columns])

JNR_E_AUD_LOG = SQ_Shortcut_to_WM_E_AUD_LOG_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_E_AUD_LOG_temp.SQ_Shortcut_to_WM_E_AUD_LOG___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_E_AUD_LOG_temp.SQ_Shortcut_to_WM_E_AUD_LOG___WM_AUD_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___AUD_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___AUD_ID as AUD_ID", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___TRAN_NBR as TRAN_NBR", \
	"JNR_SITE_PROFILE___SKU_ID as SKU_ID", \
	"JNR_SITE_PROFILE___SKU_HNDL_ATTR as SKU_HNDL_ATTR", \
	"JNR_SITE_PROFILE___CRITERIA as CRITERIA", \
	"JNR_SITE_PROFILE___SLOT_ATTR as SLOT_ATTR", \
	"JNR_SITE_PROFILE___FACTOR_TIME as FACTOR_TIME", \
	"JNR_SITE_PROFILE___CURR_SLOT as CURR_SLOT", \
	"JNR_SITE_PROFILE___NEXT_SLOT as NEXT_SLOT", \
	"JNR_SITE_PROFILE___DISTANCE as DISTANCE", \
	"JNR_SITE_PROFILE___HEIGHT as HEIGHT", \
	"JNR_SITE_PROFILE___TRVL_DIR as TRVL_DIR", \
	"JNR_SITE_PROFILE___ELEM_TIME as ELEM_TIME", \
	"JNR_SITE_PROFILE___UOM as UOM", \
	"JNR_SITE_PROFILE___CURR_LOCN_CLASS as CURR_LOCN_CLASS", \
	"JNR_SITE_PROFILE___NEXT_LOCN_CLASS as NEXT_LOCN_CLASS", \
	"JNR_SITE_PROFILE___TOT_TIME as TOT_TIME", \
	"JNR_SITE_PROFILE___TOT_UNITS as TOT_UNITS", \
	"JNR_SITE_PROFILE___UNITS_PER_GRAB as UNITS_PER_GRAB", \
	"JNR_SITE_PROFILE___LPN as LPN", \
	"JNR_SITE_PROFILE___ELEM_DESC as ELEM_DESC", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___TA_MULTIPLIER as TA_MULTIPLIER", \
	"JNR_SITE_PROFILE___ELS_TRAN_ID as ELS_TRAN_ID", \
	"JNR_SITE_PROFILE___CRIT_VAL as CRIT_VAL", \
	"JNR_SITE_PROFILE___CRIT_TIME as CRIT_TIME", \
	"JNR_SITE_PROFILE___ELM_ID as ELM_ID", \
	"JNR_SITE_PROFILE___FACTOR as FACTOR", \
	"JNR_SITE_PROFILE___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"JNR_SITE_PROFILE___MODULE_TYPE as MODULE_TYPE", \
	"JNR_SITE_PROFILE___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_SITE_PROFILE___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_SITE_PROFILE___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_SITE_PROFILE___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_SITE_PROFILE___ADDTL_TIME_ALLOW as ADDTL_TIME_ALLOW", \
	"JNR_SITE_PROFILE___SLOT_TYPE_TIME_ALLOW as SLOT_TYPE_TIME_ALLOW", \
	"JNR_SITE_PROFILE___UNIT_PICK_TIME_ALLOW as UNIT_PICK_TIME_ALLOW", \
	"JNR_SITE_PROFILE___PFD_TIME as PFD_TIME", \
	"JNR_SITE_PROFILE___VERSION_ID as VERSION_ID", \
	"JNR_SITE_PROFILE___ACT_ID as ACT_ID", \
	"JNR_SITE_PROFILE___COMPONENT_BK as COMPONENT_BK", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_AUD_ID as i_WM_AUD_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_WHSE as i_WM_WHSE", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_TRAN_NBR as i_WM_TRAN_NBR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_SKU_ID as i_WM_SKU_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_SKU_HNDL_ATTR as i_WM_SKU_HNDL_ATTR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_CRITERIA as i_WM_CRITERIA", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_SLOT_ATTR as i_WM_SLOT_ATTR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_ACT_ID as i_WM_ACT_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_LPN as i_WM_LPN", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_COMPONENT_BK as i_WM_COMPONENT_BK", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_ELM_ID as i_WM_ELM_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_FACTOR as i_WM_FACTOR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_THRUPUT_MSRMNT as i_WM_THRUPUT_MSRMNT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_MODULE_TYPE as i_WM_MODULE_TYPE", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_TA_MULTIPLIER as i_WM_TA_MULTIPLIER", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_ELS_TRAN_ID as i_WM_ELS_TRAN_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_CRIT_VAL as i_WM_CRIT_VAL", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_CRIT_TIME as i_WM_CRIT_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_CURR_SLOT as i_WM_CURR_SLOT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_NEXT_SLOT as i_WM_NEXT_SLOT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_CURR_LOCN_CLASS as i_WM_CURR_LOCN_CLASS", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_NEXT_LOCN_CLASS as i_WM_NEXT_LOCN_CLASS", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___PFD_TIME as i_PFD_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___FACTOR_TIME as i_FACTOR_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___DISTANCE as i_DISTANCE", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___HEIGHT as i_HEIGHT", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___TRVL_DIR as i_TRVL_DIR", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___ELEM_DESC as i_ELEM_DESC", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___ELEM_TIME as i_ELEM_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___UOM as i_UOM", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___TOT_TIME as i_TOT_TIME", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___TOT_UNITS as i_TOT_UNITS", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___UNITS_PER_GRAB as i_UNITS_PER_GRAB", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___UNIT_PICK_TIME_ALLOW as i_UNIT_PICK_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___SLOT_TYPE_TIME_ALLOW as i_SLOT_TYPE_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___ADDTL_TIME_ALLOW as i_ADDTL_TIME_ALLOW", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___MISC_TXT_1 as i_MISC_TXT_1", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___MISC_TXT_2 as i_MISC_TXT_2", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___MISC_NUM_1 as i_MISC_NUM_1", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___MISC_NUM_2 as i_MISC_NUM_2", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_VERSION_ID as i_WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___DELETE_FLAG as i_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___UPDATE_TSTMP as i_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_E_AUD_LOG___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 89

# for each involved DataFrame, append the dataframe name to each column
JNR_E_AUD_LOG_temp = JNR_E_AUD_LOG.toDF(*["JNR_E_AUD_LOG___" + col for col in JNR_E_AUD_LOG.columns])

FIL_UNCHANGED_RECORDS = JNR_E_AUD_LOG_temp.selectExpr( \
	"JNR_E_AUD_LOG___LOCATION_ID as LOCATION_ID", \
	"JNR_E_AUD_LOG___AUD_ID as AUD_ID", \
	"JNR_E_AUD_LOG___WHSE as WHSE", \
	"JNR_E_AUD_LOG___TRAN_NBR as TRAN_NBR", \
	"JNR_E_AUD_LOG___SKU_ID as SKU_ID", \
	"JNR_E_AUD_LOG___SKU_HNDL_ATTR as SKU_HNDL_ATTR", \
	"JNR_E_AUD_LOG___CRITERIA as CRITERIA", \
	"JNR_E_AUD_LOG___SLOT_ATTR as SLOT_ATTR", \
	"JNR_E_AUD_LOG___FACTOR_TIME as FACTOR_TIME", \
	"JNR_E_AUD_LOG___CURR_SLOT as CURR_SLOT", \
	"JNR_E_AUD_LOG___NEXT_SLOT as NEXT_SLOT", \
	"JNR_E_AUD_LOG___DISTANCE as DISTANCE", \
	"JNR_E_AUD_LOG___HEIGHT as HEIGHT", \
	"JNR_E_AUD_LOG___TRVL_DIR as TRVL_DIR", \
	"JNR_E_AUD_LOG___ELEM_TIME as ELEM_TIME", \
	"JNR_E_AUD_LOG___UOM as UOM", \
	"JNR_E_AUD_LOG___CURR_LOCN_CLASS as CURR_LOCN_CLASS", \
	"JNR_E_AUD_LOG___NEXT_LOCN_CLASS as NEXT_LOCN_CLASS", \
	"JNR_E_AUD_LOG___TOT_TIME as TOT_TIME", \
	"JNR_E_AUD_LOG___TOT_UNITS as TOT_UNITS", \
	"JNR_E_AUD_LOG___UNITS_PER_GRAB as UNITS_PER_GRAB", \
	"JNR_E_AUD_LOG___LPN as LPN", \
	"JNR_E_AUD_LOG___ELEM_DESC as ELEM_DESC", \
	"JNR_E_AUD_LOG___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_E_AUD_LOG___TA_MULTIPLIER as TA_MULTIPLIER", \
	"JNR_E_AUD_LOG___ELS_TRAN_ID as ELS_TRAN_ID", \
	"JNR_E_AUD_LOG___CRIT_VAL as CRIT_VAL", \
	"JNR_E_AUD_LOG___CRIT_TIME as CRIT_TIME", \
	"JNR_E_AUD_LOG___ELM_ID as ELM_ID", \
	"JNR_E_AUD_LOG___FACTOR as FACTOR", \
	"JNR_E_AUD_LOG___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"JNR_E_AUD_LOG___MODULE_TYPE as MODULE_TYPE", \
	"JNR_E_AUD_LOG___MISC_TXT_1 as MISC_TXT_1", \
	"JNR_E_AUD_LOG___MISC_TXT_2 as MISC_TXT_2", \
	"JNR_E_AUD_LOG___MISC_NUM_1 as MISC_NUM_1", \
	"JNR_E_AUD_LOG___MISC_NUM_2 as MISC_NUM_2", \
	"JNR_E_AUD_LOG___ADDTL_TIME_ALLOW as ADDTL_TIME_ALLOW", \
	"JNR_E_AUD_LOG___SLOT_TYPE_TIME_ALLOW as SLOT_TYPE_TIME_ALLOW", \
	"JNR_E_AUD_LOG___UNIT_PICK_TIME_ALLOW as UNIT_PICK_TIME_ALLOW", \
	"JNR_E_AUD_LOG___PFD_TIME as PFD_TIME", \
	"JNR_E_AUD_LOG___VERSION_ID as VERSION_ID", \
	"JNR_E_AUD_LOG___ACT_ID as ACT_ID", \
	"JNR_E_AUD_LOG___COMPONENT_BK as COMPONENT_BK", \
	"JNR_E_AUD_LOG___i_LOCATION_ID as i_LOCATION_ID", \
	"JNR_E_AUD_LOG___i_WM_AUD_ID as i_WM_AUD_ID", \
	"JNR_E_AUD_LOG___i_WM_WHSE as i_WM_WHSE", \
	"JNR_E_AUD_LOG___i_WM_TRAN_NBR as i_WM_TRAN_NBR", \
	"JNR_E_AUD_LOG___i_WM_SKU_ID as i_WM_SKU_ID", \
	"JNR_E_AUD_LOG___i_WM_SKU_HNDL_ATTR as i_WM_SKU_HNDL_ATTR", \
	"JNR_E_AUD_LOG___i_WM_CRITERIA as i_WM_CRITERIA", \
	"JNR_E_AUD_LOG___i_WM_SLOT_ATTR as i_WM_SLOT_ATTR", \
	"JNR_E_AUD_LOG___i_WM_ACT_ID as i_WM_ACT_ID", \
	"JNR_E_AUD_LOG___i_WM_LPN as i_WM_LPN", \
	"JNR_E_AUD_LOG___i_WM_COMPONENT_BK as i_WM_COMPONENT_BK", \
	"JNR_E_AUD_LOG___i_WM_ELM_ID as i_WM_ELM_ID", \
	"JNR_E_AUD_LOG___i_WM_FACTOR as i_WM_FACTOR", \
	"JNR_E_AUD_LOG___i_WM_THRUPUT_MSRMNT as i_WM_THRUPUT_MSRMNT", \
	"JNR_E_AUD_LOG___i_WM_MODULE_TYPE as i_WM_MODULE_TYPE", \
	"JNR_E_AUD_LOG___i_WM_TA_MULTIPLIER as i_WM_TA_MULTIPLIER", \
	"JNR_E_AUD_LOG___i_WM_ELS_TRAN_ID as i_WM_ELS_TRAN_ID", \
	"JNR_E_AUD_LOG___i_WM_CRIT_VAL as i_WM_CRIT_VAL", \
	"JNR_E_AUD_LOG___i_WM_CRIT_TIME as i_WM_CRIT_TIME", \
	"JNR_E_AUD_LOG___i_WM_CURR_SLOT as i_WM_CURR_SLOT", \
	"JNR_E_AUD_LOG___i_WM_NEXT_SLOT as i_WM_NEXT_SLOT", \
	"JNR_E_AUD_LOG___i_WM_CURR_LOCN_CLASS as i_WM_CURR_LOCN_CLASS", \
	"JNR_E_AUD_LOG___i_WM_NEXT_LOCN_CLASS as i_WM_NEXT_LOCN_CLASS", \
	"JNR_E_AUD_LOG___i_PFD_TIME as i_PFD_TIME", \
	"JNR_E_AUD_LOG___i_FACTOR_TIME as i_FACTOR_TIME", \
	"JNR_E_AUD_LOG___i_DISTANCE as i_DISTANCE", \
	"JNR_E_AUD_LOG___i_HEIGHT as i_HEIGHT", \
	"JNR_E_AUD_LOG___i_TRVL_DIR as i_TRVL_DIR", \
	"JNR_E_AUD_LOG___i_ELEM_DESC as i_ELEM_DESC", \
	"JNR_E_AUD_LOG___i_ELEM_TIME as i_ELEM_TIME", \
	"JNR_E_AUD_LOG___i_UOM as i_UOM", \
	"JNR_E_AUD_LOG___i_TOT_TIME as i_TOT_TIME", \
	"JNR_E_AUD_LOG___i_TOT_UNITS as i_TOT_UNITS", \
	"JNR_E_AUD_LOG___i_UNITS_PER_GRAB as i_UNITS_PER_GRAB", \
	"JNR_E_AUD_LOG___i_UNIT_PICK_TIME_ALLOW as i_UNIT_PICK_TIME_ALLOW", \
	"JNR_E_AUD_LOG___i_SLOT_TYPE_TIME_ALLOW as i_SLOT_TYPE_TIME_ALLOW", \
	"JNR_E_AUD_LOG___i_ADDTL_TIME_ALLOW as i_ADDTL_TIME_ALLOW", \
	"JNR_E_AUD_LOG___i_MISC_TXT_1 as i_MISC_TXT_1", \
	"JNR_E_AUD_LOG___i_MISC_TXT_2 as i_MISC_TXT_2", \
	"JNR_E_AUD_LOG___i_MISC_NUM_1 as i_MISC_NUM_1", \
	"JNR_E_AUD_LOG___i_MISC_NUM_2 as i_MISC_NUM_2", \
	"JNR_E_AUD_LOG___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"JNR_E_AUD_LOG___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_E_AUD_LOG___i_DELETE_FLAG as i_DELETE_FLAG", \
	"JNR_E_AUD_LOG___i_UPDATE_TSTMP as i_UPDATE_TSTMP", \
	"JNR_E_AUD_LOG___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("i_WM_AUD_ID IS NULL OR AUD_ID IS NULL OR (  i_WM_AUD_ID IS NOT NULL AND \
             ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 90

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
.withColumn("v_CREATE_DATE_TIME", expr("""IF(CREATE_DATE_TIME IS NULL, date'1900-01-01', CREATE_DATE_TIME)""")) \
	.withColumn("v_i_WM_CREATE_TSTMP", expr("""IF(i_WM_CREATE_TSTMP IS NULL, date'1900-01-01', i_WM_CREATE_TSTMP)"""))
             
EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___AUD_ID as AUD_ID", \
	"FIL_UNCHANGED_RECORDS___WHSE as WHSE", \
	"FIL_UNCHANGED_RECORDS___TRAN_NBR as TRAN_NBR", \
	"FIL_UNCHANGED_RECORDS___SKU_ID as SKU_ID", \
	"FIL_UNCHANGED_RECORDS___SKU_HNDL_ATTR as SKU_HNDL_ATTR", \
	"FIL_UNCHANGED_RECORDS___CRITERIA as CRITERIA", \
	"FIL_UNCHANGED_RECORDS___SLOT_ATTR as SLOT_ATTR", \
	"FIL_UNCHANGED_RECORDS___FACTOR_TIME as FACTOR_TIME", \
	"FIL_UNCHANGED_RECORDS___CURR_SLOT as CURR_SLOT", \
	"FIL_UNCHANGED_RECORDS___NEXT_SLOT as NEXT_SLOT", \
	"FIL_UNCHANGED_RECORDS___DISTANCE as DISTANCE", \
	"FIL_UNCHANGED_RECORDS___HEIGHT as HEIGHT", \
	"FIL_UNCHANGED_RECORDS___TRVL_DIR as TRVL_DIR", \
	"FIL_UNCHANGED_RECORDS___ELEM_TIME as ELEM_TIME", \
	"FIL_UNCHANGED_RECORDS___UOM as UOM", \
	"FIL_UNCHANGED_RECORDS___CURR_LOCN_CLASS as CURR_LOCN_CLASS", \
	"FIL_UNCHANGED_RECORDS___NEXT_LOCN_CLASS as NEXT_LOCN_CLASS", \
	"FIL_UNCHANGED_RECORDS___TOT_TIME as TOT_TIME", \
	"FIL_UNCHANGED_RECORDS___TOT_UNITS as TOT_UNITS", \
	"FIL_UNCHANGED_RECORDS___UNITS_PER_GRAB as UNITS_PER_GRAB", \
	"FIL_UNCHANGED_RECORDS___LPN as LPN", \
	"FIL_UNCHANGED_RECORDS___ELEM_DESC as ELEM_DESC", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___TA_MULTIPLIER as TA_MULTIPLIER", \
	"FIL_UNCHANGED_RECORDS___ELS_TRAN_ID as ELS_TRAN_ID", \
	"FIL_UNCHANGED_RECORDS___CRIT_VAL as CRIT_VAL", \
	"FIL_UNCHANGED_RECORDS___CRIT_TIME as CRIT_TIME", \
	"FIL_UNCHANGED_RECORDS___ELM_ID as ELM_ID", \
	"FIL_UNCHANGED_RECORDS___FACTOR as FACTOR", \
	"FIL_UNCHANGED_RECORDS___THRUPUT_MSRMNT as THRUPUT_MSRMNT", \
	"FIL_UNCHANGED_RECORDS___MODULE_TYPE as MODULE_TYPE", \
	"FIL_UNCHANGED_RECORDS___MISC_TXT_1 as MISC_TXT_1", \
	"FIL_UNCHANGED_RECORDS___MISC_TXT_2 as MISC_TXT_2", \
	"FIL_UNCHANGED_RECORDS___MISC_NUM_1 as MISC_NUM_1", \
	"FIL_UNCHANGED_RECORDS___MISC_NUM_2 as MISC_NUM_2", \
	"FIL_UNCHANGED_RECORDS___ADDTL_TIME_ALLOW as ADDTL_TIME_ALLOW", \
	"FIL_UNCHANGED_RECORDS___SLOT_TYPE_TIME_ALLOW as SLOT_TYPE_TIME_ALLOW", \
	"FIL_UNCHANGED_RECORDS___UNIT_PICK_TIME_ALLOW as UNIT_PICK_TIME_ALLOW", \
	"FIL_UNCHANGED_RECORDS___PFD_TIME as PFD_TIME", \
	"FIL_UNCHANGED_RECORDS___VERSION_ID as VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___ACT_ID as ACT_ID", \
	"FIL_UNCHANGED_RECORDS___COMPONENT_BK as COMPONENT_BK", \
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID as i_LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_AUD_ID as i_WM_AUD_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_WHSE as i_WM_WHSE", \
	"FIL_UNCHANGED_RECORDS___i_WM_TRAN_NBR as i_WM_TRAN_NBR", \
	"FIL_UNCHANGED_RECORDS___i_WM_SKU_ID as i_WM_SKU_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_SKU_HNDL_ATTR as i_WM_SKU_HNDL_ATTR", \
	"FIL_UNCHANGED_RECORDS___i_WM_CRITERIA as i_WM_CRITERIA", \
	"FIL_UNCHANGED_RECORDS___i_WM_SLOT_ATTR as i_WM_SLOT_ATTR", \
	"FIL_UNCHANGED_RECORDS___i_WM_ACT_ID as i_WM_ACT_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_LPN as i_WM_LPN", \
	"FIL_UNCHANGED_RECORDS___i_WM_COMPONENT_BK as i_WM_COMPONENT_BK", \
	"FIL_UNCHANGED_RECORDS___i_WM_ELM_ID as i_WM_ELM_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_FACTOR as i_WM_FACTOR", \
	"FIL_UNCHANGED_RECORDS___i_WM_THRUPUT_MSRMNT as i_WM_THRUPUT_MSRMNT", \
	"FIL_UNCHANGED_RECORDS___i_WM_MODULE_TYPE as i_WM_MODULE_TYPE", \
	"FIL_UNCHANGED_RECORDS___i_WM_TA_MULTIPLIER as i_WM_TA_MULTIPLIER", \
	"FIL_UNCHANGED_RECORDS___i_WM_ELS_TRAN_ID as i_WM_ELS_TRAN_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_CRIT_VAL as i_WM_CRIT_VAL", \
	"FIL_UNCHANGED_RECORDS___i_WM_CRIT_TIME as i_WM_CRIT_TIME", \
	"FIL_UNCHANGED_RECORDS___i_WM_CURR_SLOT as i_WM_CURR_SLOT", \
	"FIL_UNCHANGED_RECORDS___i_WM_NEXT_SLOT as i_WM_NEXT_SLOT", \
	"FIL_UNCHANGED_RECORDS___i_WM_CURR_LOCN_CLASS as i_WM_CURR_LOCN_CLASS", \
	"FIL_UNCHANGED_RECORDS___i_WM_NEXT_LOCN_CLASS as i_WM_NEXT_LOCN_CLASS", \
	"FIL_UNCHANGED_RECORDS___i_PFD_TIME as i_PFD_TIME", \
	"FIL_UNCHANGED_RECORDS___i_FACTOR_TIME as i_FACTOR_TIME", \
	"FIL_UNCHANGED_RECORDS___i_DISTANCE as i_DISTANCE", \
	"FIL_UNCHANGED_RECORDS___i_HEIGHT as i_HEIGHT", \
	"FIL_UNCHANGED_RECORDS___i_TRVL_DIR as i_TRVL_DIR", \
	"FIL_UNCHANGED_RECORDS___i_ELEM_DESC as i_ELEM_DESC", \
	"FIL_UNCHANGED_RECORDS___i_ELEM_TIME as i_ELEM_TIME", \
	"FIL_UNCHANGED_RECORDS___i_UOM as i_UOM", \
	"FIL_UNCHANGED_RECORDS___i_TOT_TIME as i_TOT_TIME", \
	"FIL_UNCHANGED_RECORDS___i_TOT_UNITS as i_TOT_UNITS", \
	"FIL_UNCHANGED_RECORDS___i_UNITS_PER_GRAB as i_UNITS_PER_GRAB", \
	"FIL_UNCHANGED_RECORDS___i_UNIT_PICK_TIME_ALLOW as i_UNIT_PICK_TIME_ALLOW", \
	"FIL_UNCHANGED_RECORDS___i_SLOT_TYPE_TIME_ALLOW as i_SLOT_TYPE_TIME_ALLOW", \
	"FIL_UNCHANGED_RECORDS___i_ADDTL_TIME_ALLOW as i_ADDTL_TIME_ALLOW", \
	"FIL_UNCHANGED_RECORDS___i_MISC_TXT_1 as i_MISC_TXT_1", \
	"FIL_UNCHANGED_RECORDS___i_MISC_TXT_2 as i_MISC_TXT_2", \
	"FIL_UNCHANGED_RECORDS___i_MISC_NUM_1 as i_MISC_NUM_1", \
	"FIL_UNCHANGED_RECORDS___i_MISC_NUM_2 as i_MISC_NUM_2", \
	"FIL_UNCHANGED_RECORDS___i_WM_VERSION_ID as i_WM_VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___AUD_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_AUD_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___AUD_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_AUD_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_RECORDS___AUD_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_AUD_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___v_i_WM_CREATE_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATE_DATE_TIME, 'UPDATE', NULL)) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 47

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID1", \
	"EXP_UPD_VALIDATOR___AUD_ID as AUD_ID1", \
	"EXP_UPD_VALIDATOR___WHSE as WHSE1", \
	"EXP_UPD_VALIDATOR___TRAN_NBR as TRAN_NBR1", \
	"EXP_UPD_VALIDATOR___SKU_ID as SKU_ID1", \
	"EXP_UPD_VALIDATOR___SKU_HNDL_ATTR as SKU_HNDL_ATTR1", \
	"EXP_UPD_VALIDATOR___CRITERIA as CRITERIA1", \
	"EXP_UPD_VALIDATOR___SLOT_ATTR as SLOT_ATTR1", \
	"EXP_UPD_VALIDATOR___FACTOR_TIME as FACTOR_TIME1", \
	"EXP_UPD_VALIDATOR___CURR_SLOT as CURR_SLOT1", \
	"EXP_UPD_VALIDATOR___NEXT_SLOT as NEXT_SLOT1", \
	"EXP_UPD_VALIDATOR___DISTANCE as DISTANCE1", \
	"EXP_UPD_VALIDATOR___HEIGHT as HEIGHT1", \
	"EXP_UPD_VALIDATOR___TRVL_DIR as TRVL_DIR1", \
	"EXP_UPD_VALIDATOR___ELEM_TIME as ELEM_TIME1", \
	"EXP_UPD_VALIDATOR___UOM as UOM1", \
	"EXP_UPD_VALIDATOR___CURR_LOCN_CLASS as CURR_LOCN_CLASS1", \
	"EXP_UPD_VALIDATOR___NEXT_LOCN_CLASS as NEXT_LOCN_CLASS1", \
	"EXP_UPD_VALIDATOR___TOT_TIME as TOT_TIME1", \
	"EXP_UPD_VALIDATOR___TOT_UNITS as TOT_UNITS1", \
	"EXP_UPD_VALIDATOR___UNITS_PER_GRAB as UNITS_PER_GRAB1", \
	"EXP_UPD_VALIDATOR___LPN as LPN1", \
	"EXP_UPD_VALIDATOR___ELEM_DESC as ELEM_DESC1", \
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME1", \
	"EXP_UPD_VALIDATOR___TA_MULTIPLIER as TA_MULTIPLIER1", \
	"EXP_UPD_VALIDATOR___ELS_TRAN_ID as ELS_TRAN_ID1", \
	"EXP_UPD_VALIDATOR___CRIT_VAL as CRIT_VAL1", \
	"EXP_UPD_VALIDATOR___CRIT_TIME as CRIT_TIME1", \
	"EXP_UPD_VALIDATOR___ELM_ID as ELM_ID1", \
	"EXP_UPD_VALIDATOR___FACTOR as FACTOR1", \
	"EXP_UPD_VALIDATOR___THRUPUT_MSRMNT as THRUPUT_MSRMNT1", \
	"EXP_UPD_VALIDATOR___MODULE_TYPE as MODULE_TYPE1", \
	"EXP_UPD_VALIDATOR___MISC_TXT_1 as MISC_TXT_11", \
	"EXP_UPD_VALIDATOR___MISC_TXT_2 as MISC_TXT_21", \
	"EXP_UPD_VALIDATOR___MISC_NUM_1 as MISC_NUM_11", \
	"EXP_UPD_VALIDATOR___MISC_NUM_2 as MISC_NUM_21", \
	"EXP_UPD_VALIDATOR___ADDTL_TIME_ALLOW as ADDTL_TIME_ALLOW1", \
	"EXP_UPD_VALIDATOR___SLOT_TYPE_TIME_ALLOW as SLOT_TYPE_TIME_ALLOW1", \
	"EXP_UPD_VALIDATOR___UNIT_PICK_TIME_ALLOW as UNIT_PICK_TIME_ALLOW1", \
	"EXP_UPD_VALIDATOR___PFD_TIME as PFD_TIME1", \
	"EXP_UPD_VALIDATOR___VERSION_ID as VERSION_ID1", \
	"EXP_UPD_VALIDATOR___ACT_ID as ACT_ID1", \
	"EXP_UPD_VALIDATOR___COMPONENT_BK as COMPONENT_BK1", \
	"EXP_UPD_VALIDATOR___DELETE_FLAG as DELETE_FLAG1", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP1", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP1", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit('INSERT')), lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_E_AUD_LOG1, type TARGET 
# COLUMN COUNT: 46

Shortcut_to_WM_E_AUD_LOG1 = UPD_INSERT_UPDATE.selectExpr( 
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID", 
	"CAST(AUD_ID1 AS BIGINT) as WM_AUD_ID", 
	"CAST(WHSE1 AS STRING) as WM_WHSE", 
	"CAST(TRAN_NBR1 AS BIGINT) as WM_TRAN_NBR", 
	"CAST(SKU_ID1 AS STRING) as WM_SKU_ID", 
	"CAST(SKU_HNDL_ATTR1 AS STRING) as WM_SKU_HNDL_ATTR", 
	"CAST(CRITERIA1 AS STRING) as WM_CRITERIA", 
	"CAST(SLOT_ATTR1 AS STRING) as WM_SLOT_ATTR", 
	"CAST(ACT_ID1 AS BIGINT) as WM_ACT_ID", 
	"CAST(LPN1 AS STRING) as WM_LPN", 
	"CAST(COMPONENT_BK1 AS STRING) as WM_COMPONENT_BK", 
	"CAST(ELM_ID1 AS BIGINT) as WM_ELM_ID", 
	"CAST(FACTOR1 AS STRING) as WM_FACTOR", 
	"CAST(THRUPUT_MSRMNT1 AS STRING) as WM_THRUPUT_MSRMNT", 
	"CAST(MODULE_TYPE1 AS STRING) as WM_MODULE_TYPE", 
	"CAST(TA_MULTIPLIER1 AS BIGINT) as WM_TA_MULTIPLIER", 
	"CAST(ELS_TRAN_ID1 AS BIGINT) as WM_ELS_TRAN_ID", 
	"CAST(CRIT_VAL1 AS STRING) as WM_CRIT_VAL", 
	"CAST(CRIT_TIME1 AS BIGINT) as WM_CRIT_TIME", 
	"CAST(CURR_SLOT1 AS STRING) as WM_CURR_SLOT", 
	"CAST(NEXT_SLOT1 AS STRING) as WM_NEXT_SLOT", 
	"CAST(CURR_LOCN_CLASS1 AS STRING) as WM_CURR_LOCN_CLASS", 
	"CAST(NEXT_LOCN_CLASS1 AS STRING) as WM_NEXT_LOCN_CLASS", 
	"CAST(PFD_TIME1 AS BIGINT) as PFD_TIME", 
	"CAST(FACTOR_TIME1 AS BIGINT) as FACTOR_TIME", 
	"CAST(DISTANCE1 AS BIGINT) as DISTANCE", 
	"CAST(HEIGHT1 AS BIGINT) as HEIGHT", 
	"CAST(TRVL_DIR1 AS STRING) as TRVL_DIR", 
	"CAST(ELEM_DESC1 AS STRING) as ELEM_DESC", 
	"CAST(ELEM_TIME1 AS BIGINT) as ELEM_TIME", 
	"CAST(UOM1 AS STRING) as UOM", 
	"CAST(TOT_TIME1 AS BIGINT) as TOT_TIME", 
	"CAST(TOT_UNITS1 AS BIGINT) as TOT_UNITS", 
	"CAST(UNITS_PER_GRAB1 AS BIGINT) as UNITS_PER_GRAB", 
	"CAST(UNIT_PICK_TIME_ALLOW1 AS BIGINT) as UNIT_PICK_TIME_ALLOW", 
	"CAST(SLOT_TYPE_TIME_ALLOW1 AS BIGINT) as SLOT_TYPE_TIME_ALLOW", 
	"CAST(ADDTL_TIME_ALLOW1 AS BIGINT) as ADDTL_TIME_ALLOW", 
	"CAST(MISC_TXT_11 AS STRING) as MISC_TXT_1", 
	"CAST(MISC_TXT_21 AS STRING) as MISC_TXT_2", 
	"CAST(MISC_NUM_11 AS BIGINT) as MISC_NUM_1", 
	"CAST(MISC_NUM_21 AS BIGINT) as MISC_NUM_2", 
	"CAST(VERSION_ID1 AS BIGINT) as WM_VERSION_ID", 
	"CAST(CREATE_DATE_TIME1 AS TIMESTAMP) as WM_CREATE_TSTMP", 
	"CAST(DELETE_FLAG1 AS BIGINT) as DELETE_FLAG", 
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_AUD_ID = target.WM_AUD_ID"""
#   refined_perf_table = "WM_E_AUD_LOG"
  executeMerge(Shortcut_to_WM_E_AUD_LOG1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_E_AUD_LOG", "WM_E_AUD_LOG", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_E_AUD_LOG", "WM_E_AUD_LOG","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	