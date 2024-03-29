#Code converted on 2023-06-26 10:16:46
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
refined_perf_table = f"{refine}.WM_LOCN_HDR"
raw_perf_table = f"{raw}.WM_LOCN_HDR_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LOCN_HDR, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_WM_LOCN_HDR = spark.sql(f"""SELECT
LOCATION_ID,
WM_LOCN_HDR_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_LOCN_HDR_ID IN (SELECT LOCN_HDR_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LOCN_HDR_PRE, type SOURCE 
# COLUMN COUNT: 47

SQ_Shortcut_to_WM_LOCN_HDR_PRE = spark.sql(f"""SELECT
DC_NBR,
LOCN_HDR_ID,
LOCN_ID,
WHSE,
LOCN_CLASS,
LOCN_BRCD,
AREA,
ZONE,
AISLE,
BAY,
LVL,
POSN,
DSP_LOCN,
LOCN_PICK_SEQ,
SKU_DEDCTN_TYPE,
SLOT_TYPE,
PUTWY_ZONE,
PULL_ZONE,
PICK_DETRM_ZONE,
LEN,
WIDTH,
HT,
X_COORD,
Y_COORD,
Z_COORD,
WORK_GRP,
WORK_AREA,
LAST_FROZN_DATE_TIME,
LAST_CNT_DATE_TIME,
CYCLE_CNT_PENDIN,
PRT_LABEL_FLAG,
TRAVEL_AISLE,
TRAVEL_ZONE,
STORAGE_UOM,
PICK_UOM,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
SLOT_UNUSABLE,
CHECK_DIGIT,
VOCO_INTRNL_REVERSE_BRCD,
WM_VERSION_ID,
LOCN_PUTWY_SEQ,
LOCN_DYN_ASSGN_SEQ,
CREATED_DTTM,
LAST_UPDATED_DTTM,
FACILITY_ID
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 47

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LOCN_HDR_PRE_temp = SQ_Shortcut_to_WM_LOCN_HDR_PRE.toDF(*["SQ_Shortcut_to_WM_LOCN_HDR_PRE___" + col for col in SQ_Shortcut_to_WM_LOCN_HDR_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_LOCN_HDR_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LOCN_HDR_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_HDR_ID as LOCN_HDR_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_ID as LOCN_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___WHSE as WHSE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_CLASS as LOCN_CLASS", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_BRCD as LOCN_BRCD", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___AREA as AREA", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___ZONE as ZONE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___AISLE as AISLE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___BAY as BAY", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LVL as LVL", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___POSN as POSN", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___DSP_LOCN as DSP_LOCN", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_PICK_SEQ as LOCN_PICK_SEQ", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___SKU_DEDCTN_TYPE as SKU_DEDCTN_TYPE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___SLOT_TYPE as SLOT_TYPE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___PUTWY_ZONE as PUTWY_ZONE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___PULL_ZONE as PULL_ZONE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___PICK_DETRM_ZONE as PICK_DETRM_ZONE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LEN as LEN", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___WIDTH as WIDTH", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___HT as HT", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___X_COORD as X_COORD", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___Y_COORD as Y_COORD", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___Z_COORD as Z_COORD", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___WORK_GRP as WORK_GRP", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___WORK_AREA as WORK_AREA", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LAST_FROZN_DATE_TIME as LAST_FROZN_DATE_TIME", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LAST_CNT_DATE_TIME as LAST_CNT_DATE_TIME", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___CYCLE_CNT_PENDIN as CYCLE_CNT_PENDIN", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___PRT_LABEL_FLAG as PRT_LABEL_FLAG", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___TRAVEL_AISLE as TRAVEL_AISLE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___TRAVEL_ZONE as TRAVEL_ZONE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___STORAGE_UOM as STORAGE_UOM", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___PICK_UOM as PICK_UOM", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___SLOT_UNUSABLE as SLOT_UNUSABLE", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___CHECK_DIGIT as CHECK_DIGIT", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___VOCO_INTRNL_REVERSE_BRCD as VOCO_INTRNL_REVERSE_BRCD", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___WM_VERSION_ID as WM_VERSION_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_PUTWY_SEQ as LOCN_PUTWY_SEQ", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LOCN_DYN_ASSGN_SEQ as LOCN_DYN_ASSGN_SEQ", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_LOCN_HDR_PRE___FACILITY_ID as FACILITY_ID" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 49

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LOCN_HDR, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 54

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_LOCN_HDR_temp = SQ_Shortcut_to_WM_LOCN_HDR.toDF(*["SQ_Shortcut_to_WM_LOCN_HDR___" + col for col in SQ_Shortcut_to_WM_LOCN_HDR.columns])

JNR_WM_LOCN_HDR = SQ_Shortcut_to_WM_LOCN_HDR_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LOCN_HDR_temp.SQ_Shortcut_to_WM_LOCN_HDR___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LOCN_HDR_temp.SQ_Shortcut_to_WM_LOCN_HDR___WM_LOCN_HDR_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCN_HDR_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___LOCN_HDR_ID as LOCN_HDR_ID", \
	"JNR_SITE_PROFILE___LOCN_ID as LOCN_ID", \
	"JNR_SITE_PROFILE___WHSE as WHSE", \
	"JNR_SITE_PROFILE___LOCN_CLASS as LOCN_CLASS", \
	"JNR_SITE_PROFILE___LOCN_BRCD as LOCN_BRCD", \
	"JNR_SITE_PROFILE___AREA as AREA", \
	"JNR_SITE_PROFILE___ZONE as ZONE", \
	"JNR_SITE_PROFILE___AISLE as AISLE", \
	"JNR_SITE_PROFILE___BAY as BAY", \
	"JNR_SITE_PROFILE___LVL as LVL", \
	"JNR_SITE_PROFILE___POSN as POSN", \
	"JNR_SITE_PROFILE___DSP_LOCN as DSP_LOCN", \
	"JNR_SITE_PROFILE___LOCN_PICK_SEQ as LOCN_PICK_SEQ", \
	"JNR_SITE_PROFILE___SKU_DEDCTN_TYPE as SKU_DEDCTN_TYPE", \
	"JNR_SITE_PROFILE___SLOT_TYPE as SLOT_TYPE", \
	"JNR_SITE_PROFILE___PUTWY_ZONE as PUTWY_ZONE", \
	"JNR_SITE_PROFILE___PULL_ZONE as PULL_ZONE", \
	"JNR_SITE_PROFILE___PICK_DETRM_ZONE as PICK_DETRM_ZONE", \
	"JNR_SITE_PROFILE___LEN as LEN", \
	"JNR_SITE_PROFILE___WIDTH as WIDTH", \
	"JNR_SITE_PROFILE___HT as HT", \
	"JNR_SITE_PROFILE___X_COORD as X_COORD", \
	"JNR_SITE_PROFILE___Y_COORD as Y_COORD", \
	"JNR_SITE_PROFILE___Z_COORD as Z_COORD", \
	"JNR_SITE_PROFILE___WORK_GRP as WORK_GRP", \
	"JNR_SITE_PROFILE___WORK_AREA as WORK_AREA", \
	"JNR_SITE_PROFILE___LAST_FROZN_DATE_TIME as LAST_FROZN_DATE_TIME", \
	"JNR_SITE_PROFILE___LAST_CNT_DATE_TIME as LAST_CNT_DATE_TIME", \
	"JNR_SITE_PROFILE___CYCLE_CNT_PENDIN as CYCLE_CNT_PENDIN", \
	"JNR_SITE_PROFILE___PRT_LABEL_FLAG as PRT_LABEL_FLAG", \
	"JNR_SITE_PROFILE___TRAVEL_AISLE as TRAVEL_AISLE", \
	"JNR_SITE_PROFILE___TRAVEL_ZONE as TRAVEL_ZONE", \
	"JNR_SITE_PROFILE___STORAGE_UOM as STORAGE_UOM", \
	"JNR_SITE_PROFILE___PICK_UOM as PICK_UOM", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___SLOT_UNUSABLE as SLOT_UNUSABLE", \
	"JNR_SITE_PROFILE___CHECK_DIGIT as CHECK_DIGIT", \
	"JNR_SITE_PROFILE___VOCO_INTRNL_REVERSE_BRCD as VOCO_INTRNL_REVERSE_BRCD", \
	"JNR_SITE_PROFILE___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_SITE_PROFILE___LOCN_PUTWY_SEQ as LOCN_PUTWY_SEQ", \
	"JNR_SITE_PROFILE___LOCN_DYN_ASSGN_SEQ as LOCN_DYN_ASSGN_SEQ", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___FACILITY_ID as FACILITY_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR___WM_LOCN_HDR_ID as i_WM_LOCN_HDR_ID", \
	"SQ_Shortcut_to_WM_LOCN_HDR___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_HDR___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_HDR___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_HDR___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_LOCN_HDR___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 53

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LOCN_HDR_temp = JNR_WM_LOCN_HDR.toDF(*["JNR_WM_LOCN_HDR___" + col for col in JNR_WM_LOCN_HDR.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_LOCN_HDR_temp.selectExpr( \
	"JNR_WM_LOCN_HDR___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LOCN_HDR___LOCN_HDR_ID as LOCN_HDR_ID", \
	"JNR_WM_LOCN_HDR___LOCN_ID as LOCN_ID", \
	"JNR_WM_LOCN_HDR___WHSE as WHSE", \
	"JNR_WM_LOCN_HDR___LOCN_CLASS as LOCN_CLASS", \
	"JNR_WM_LOCN_HDR___LOCN_BRCD as LOCN_BRCD", \
	"JNR_WM_LOCN_HDR___AREA as AREA", \
	"JNR_WM_LOCN_HDR___ZONE as ZONE", \
	"JNR_WM_LOCN_HDR___AISLE as AISLE", \
	"JNR_WM_LOCN_HDR___BAY as BAY", \
	"JNR_WM_LOCN_HDR___LVL as LVL", \
	"JNR_WM_LOCN_HDR___POSN as POSN", \
	"JNR_WM_LOCN_HDR___DSP_LOCN as DSP_LOCN", \
	"JNR_WM_LOCN_HDR___LOCN_PICK_SEQ as LOCN_PICK_SEQ", \
	"JNR_WM_LOCN_HDR___SKU_DEDCTN_TYPE as SKU_DEDCTN_TYPE", \
	"JNR_WM_LOCN_HDR___SLOT_TYPE as SLOT_TYPE", \
	"JNR_WM_LOCN_HDR___PUTWY_ZONE as PUTWY_ZONE", \
	"JNR_WM_LOCN_HDR___PULL_ZONE as PULL_ZONE", \
	"JNR_WM_LOCN_HDR___PICK_DETRM_ZONE as PICK_DETRM_ZONE", \
	"JNR_WM_LOCN_HDR___LEN as LEN", \
	"JNR_WM_LOCN_HDR___WIDTH as WIDTH", \
	"JNR_WM_LOCN_HDR___HT as HT", \
	"JNR_WM_LOCN_HDR___X_COORD as X_COORD", \
	"JNR_WM_LOCN_HDR___Y_COORD as Y_COORD", \
	"JNR_WM_LOCN_HDR___Z_COORD as Z_COORD", \
	"JNR_WM_LOCN_HDR___WORK_GRP as WORK_GRP", \
	"JNR_WM_LOCN_HDR___WORK_AREA as WORK_AREA", \
	"JNR_WM_LOCN_HDR___LAST_FROZN_DATE_TIME as LAST_FROZN_DATE_TIME", \
	"JNR_WM_LOCN_HDR___LAST_CNT_DATE_TIME as LAST_CNT_DATE_TIME", \
	"JNR_WM_LOCN_HDR___CYCLE_CNT_PENDIN as CYCLE_CNT_PENDIN", \
	"JNR_WM_LOCN_HDR___PRT_LABEL_FLAG as PRT_LABEL_FLAG", \
	"JNR_WM_LOCN_HDR___TRAVEL_AISLE as TRAVEL_AISLE", \
	"JNR_WM_LOCN_HDR___TRAVEL_ZONE as TRAVEL_ZONE", \
	"JNR_WM_LOCN_HDR___STORAGE_UOM as STORAGE_UOM", \
	"JNR_WM_LOCN_HDR___PICK_UOM as PICK_UOM", \
	"JNR_WM_LOCN_HDR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_LOCN_HDR___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_LOCN_HDR___USER_ID as USER_ID", \
	"JNR_WM_LOCN_HDR___SLOT_UNUSABLE as SLOT_UNUSABLE", \
	"JNR_WM_LOCN_HDR___CHECK_DIGIT as CHECK_DIGIT", \
	"JNR_WM_LOCN_HDR___VOCO_INTRNL_REVERSE_BRCD as VOCO_INTRNL_REVERSE_BRCD", \
	"JNR_WM_LOCN_HDR___WM_VERSION_ID as WM_VERSION_ID", \
	"JNR_WM_LOCN_HDR___LOCN_PUTWY_SEQ as LOCN_PUTWY_SEQ", \
	"JNR_WM_LOCN_HDR___LOCN_DYN_ASSGN_SEQ as LOCN_DYN_ASSGN_SEQ", \
	"JNR_WM_LOCN_HDR___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_LOCN_HDR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_LOCN_HDR___FACILITY_ID as FACILITY_ID", \
	"JNR_WM_LOCN_HDR___i_WM_LOCN_HDR_ID as i_WM_LOCN_HDR_ID", \
	"JNR_WM_LOCN_HDR___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_LOCN_HDR___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_LOCN_HDR___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_LOCN_HDR___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_LOCN_HDR___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("i_WM_LOCN_HDR_ID is Null OR (  i_WM_LOCN_HDR_ID is NOT Null AND ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01') OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01') OR COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 53

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___LOCN_HDR_ID as LOCN_HDR_ID", \
	"FIL_UNCHANGED_RECORDS___LOCN_ID as LOCN_ID", \
	"FIL_UNCHANGED_RECORDS___WHSE as WHSE", \
	"FIL_UNCHANGED_RECORDS___LOCN_CLASS as LOCN_CLASS", \
	"FIL_UNCHANGED_RECORDS___LOCN_BRCD as LOCN_BRCD", \
	"FIL_UNCHANGED_RECORDS___AREA as AREA", \
	"FIL_UNCHANGED_RECORDS___ZONE as ZONE", \
	"FIL_UNCHANGED_RECORDS___AISLE as AISLE", \
	"FIL_UNCHANGED_RECORDS___BAY as BAY", \
	"FIL_UNCHANGED_RECORDS___LVL as LVL", \
	"FIL_UNCHANGED_RECORDS___POSN as POSN", \
	"FIL_UNCHANGED_RECORDS___DSP_LOCN as DSP_LOCN", \
	"FIL_UNCHANGED_RECORDS___LOCN_PICK_SEQ as LOCN_PICK_SEQ", \
	"FIL_UNCHANGED_RECORDS___SKU_DEDCTN_TYPE as SKU_DEDCTN_TYPE", \
	"FIL_UNCHANGED_RECORDS___SLOT_TYPE as SLOT_TYPE", \
	"FIL_UNCHANGED_RECORDS___PUTWY_ZONE as PUTWY_ZONE", \
	"FIL_UNCHANGED_RECORDS___PULL_ZONE as PULL_ZONE", \
	"FIL_UNCHANGED_RECORDS___PICK_DETRM_ZONE as PICK_DETRM_ZONE", \
	"FIL_UNCHANGED_RECORDS___LEN as LEN", \
	"FIL_UNCHANGED_RECORDS___WIDTH as WIDTH", \
	"FIL_UNCHANGED_RECORDS___HT as HT", \
	"FIL_UNCHANGED_RECORDS___X_COORD as X_COORD", \
	"FIL_UNCHANGED_RECORDS___Y_COORD as Y_COORD", \
	"FIL_UNCHANGED_RECORDS___Z_COORD as Z_COORD", \
	"FIL_UNCHANGED_RECORDS___WORK_GRP as WORK_GRP", \
	"FIL_UNCHANGED_RECORDS___WORK_AREA as WORK_AREA", \
	"FIL_UNCHANGED_RECORDS___LAST_FROZN_DATE_TIME as LAST_FROZN_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___LAST_CNT_DATE_TIME as LAST_CNT_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___CYCLE_CNT_PENDIN as CYCLE_CNT_PENDIN", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___CYCLE_CNT_PENDIN)) IN ('Y', '1') THEN '1' ELSE '0' END as o_CYCLE_CNT_PENDIN", \
	"FIL_UNCHANGED_RECORDS___PRT_LABEL_FLAG as PRT_LABEL_FLAG", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___PRT_LABEL_FLAG)) IN ('Y', '1') THEN '1' ELSE '0' END as o_PRT_LABEL_FLAG", \
	"FIL_UNCHANGED_RECORDS___TRAVEL_AISLE as TRAVEL_AISLE", \
	"FIL_UNCHANGED_RECORDS___TRAVEL_ZONE as TRAVEL_ZONE", \
	"FIL_UNCHANGED_RECORDS___STORAGE_UOM as STORAGE_UOM", \
	"FIL_UNCHANGED_RECORDS___PICK_UOM as PICK_UOM", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___USER_ID as USER_ID", \
	"FIL_UNCHANGED_RECORDS___SLOT_UNUSABLE as SLOT_UNUSABLE", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_RECORDS___SLOT_UNUSABLE)) IN ('Y', '1') THEN '1' ELSE '0' END as o_SLOT_UNUSABLE", \
	"FIL_UNCHANGED_RECORDS___CHECK_DIGIT as CHECK_DIGIT", \
	"FIL_UNCHANGED_RECORDS___VOCO_INTRNL_REVERSE_BRCD as VOCO_INTRNL_REVERSE_BRCD", \
	"FIL_UNCHANGED_RECORDS___WM_VERSION_ID as WM_VERSION_ID", \
	"FIL_UNCHANGED_RECORDS___LOCN_PUTWY_SEQ as LOCN_PUTWY_SEQ", \
	"FIL_UNCHANGED_RECORDS___LOCN_DYN_ASSGN_SEQ as LOCN_DYN_ASSGN_SEQ", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___FACILITY_ID as FACILITY_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_WM_LOCN_HDR_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 50

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___LOCN_HDR_ID as LOCN_HDR_ID", \
	"EXP_UPD_VALIDATOR___LOCN_ID as LOCN_ID", \
	"EXP_UPD_VALIDATOR___WHSE as WHSE", \
	"EXP_UPD_VALIDATOR___LOCN_CLASS as LOCN_CLASS", \
	"EXP_UPD_VALIDATOR___LOCN_BRCD as LOCN_BRCD", \
	"EXP_UPD_VALIDATOR___AREA as AREA", \
	"EXP_UPD_VALIDATOR___ZONE as ZONE", \
	"EXP_UPD_VALIDATOR___AISLE as AISLE", \
	"EXP_UPD_VALIDATOR___BAY as BAY", \
	"EXP_UPD_VALIDATOR___LVL as LVL", \
	"EXP_UPD_VALIDATOR___POSN as POSN", \
	"EXP_UPD_VALIDATOR___DSP_LOCN as DSP_LOCN", \
	"EXP_UPD_VALIDATOR___LOCN_PICK_SEQ as LOCN_PICK_SEQ", \
	"EXP_UPD_VALIDATOR___SKU_DEDCTN_TYPE as SKU_DEDCTN_TYPE", \
	"EXP_UPD_VALIDATOR___SLOT_TYPE as SLOT_TYPE", \
	"EXP_UPD_VALIDATOR___PUTWY_ZONE as PUTWY_ZONE", \
	"EXP_UPD_VALIDATOR___PULL_ZONE as PULL_ZONE", \
	"EXP_UPD_VALIDATOR___PICK_DETRM_ZONE as PICK_DETRM_ZONE", \
	"EXP_UPD_VALIDATOR___LEN as LEN", \
	"EXP_UPD_VALIDATOR___WIDTH as WIDTH", \
	"EXP_UPD_VALIDATOR___HT as HT", \
	"EXP_UPD_VALIDATOR___X_COORD as X_COORD", \
	"EXP_UPD_VALIDATOR___Y_COORD as Y_COORD", \
	"EXP_UPD_VALIDATOR___Z_COORD as Z_COORD", \
	"EXP_UPD_VALIDATOR___WORK_GRP as WORK_GRP", \
	"EXP_UPD_VALIDATOR___WORK_AREA as WORK_AREA", \
	"EXP_UPD_VALIDATOR___LAST_FROZN_DATE_TIME as LAST_FROZN_DATE_TIME", \
	"EXP_UPD_VALIDATOR___LAST_CNT_DATE_TIME as LAST_CNT_DATE_TIME", \
	"EXP_UPD_VALIDATOR___o_CYCLE_CNT_PENDIN as o_CYCLE_CNT_PENDIN", \
	"EXP_UPD_VALIDATOR___o_PRT_LABEL_FLAG as o_PRT_LABEL_FLAG", \
	"EXP_UPD_VALIDATOR___TRAVEL_AISLE as TRAVEL_AISLE", \
	"EXP_UPD_VALIDATOR___TRAVEL_ZONE as TRAVEL_ZONE", \
	"EXP_UPD_VALIDATOR___STORAGE_UOM as STORAGE_UOM", \
	"EXP_UPD_VALIDATOR___PICK_UOM as PICK_UOM", \
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPD_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_UPD_VALIDATOR___USER_ID as USER_ID", \
	"EXP_UPD_VALIDATOR___o_SLOT_UNUSABLE as o_SLOT_UNUSABLE", \
	"EXP_UPD_VALIDATOR___CHECK_DIGIT as CHECK_DIGIT", \
	"EXP_UPD_VALIDATOR___VOCO_INTRNL_REVERSE_BRCD as VOCO_INTRNL_REVERSE_BRCD", \
	"EXP_UPD_VALIDATOR___WM_VERSION_ID as WM_VERSION_ID", \
	"EXP_UPD_VALIDATOR___LOCN_PUTWY_SEQ as LOCN_PUTWY_SEQ", \
	"EXP_UPD_VALIDATOR___LOCN_DYN_ASSGN_SEQ as LOCN_DYN_ASSGN_SEQ", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___FACILITY_ID as FACILITY_ID", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LOCN_HDR1, type TARGET 
# COLUMN COUNT: 49

Shortcut_to_WM_LOCN_HDR1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(LOCN_HDR_ID AS INT) as WM_LOCN_HDR_ID",
	"CAST(LOCN_ID AS STRING) as WM_LOCN_ID",
	"CAST(WHSE AS STRING) as WM_WHSE",
	"CAST(FACILITY_ID AS INT) as WM_FACILITY_ID",
	"CAST(LOCN_CLASS AS STRING) as WM_LOCN_CLASS",
	"CAST(LOCN_BRCD AS STRING) as WM_LOCN_BRCD",
	"CAST(DSP_LOCN AS STRING) as WM_DSP_LOCN",
	"CAST(SKU_DEDCTN_TYPE AS STRING) as WM_SKU_DEDCTN_TYPE",
	"CAST(SLOT_TYPE AS STRING) as WM_SLOT_TYPE",
	"CAST(PUTWY_ZONE AS STRING) as WM_PUTWY_ZONE",
	"CAST(PULL_ZONE AS STRING) as WM_PULL_ZONE",
	"CAST(PICK_DETRM_ZONE AS STRING) as WM_PICK_DETRM_ZONE",
	"CAST(LOCN_PICK_SEQ AS STRING) as WM_LOCN_PICK_SEQ",
	"CAST(LOCN_PUTWY_SEQ AS STRING) as WM_LOCN_PUTWY_SEQ",
	"CAST(LOCN_DYN_ASSGN_SEQ AS STRING) as WM_LOCN_DYN_ASSGN_SEQ",
	"CAST(WORK_GRP AS STRING) as WM_WORK_GRP",
	"CAST(WORK_AREA AS STRING) as WM_WORK_AREA",
	"CAST(VOCO_INTRNL_REVERSE_BRCD AS STRING) as WM_VOCO_INTRNL_REVERSE_BRCD",
	"CAST(CHECK_DIGIT AS STRING) as WM_CHECK_DIGIT",
	"CAST(LAST_FROZN_DATE_TIME AS TIMESTAMP) as WM_LAST_FROZN_TSTMP",
	"CAST(LAST_CNT_DATE_TIME AS TIMESTAMP) as WM_LAST_CNT_TSTMP",
	"CAST(o_CYCLE_CNT_PENDIN AS TINYINT) as CYCLE_CNT_PENDING_FLAG",
	"CAST(o_PRT_LABEL_FLAG AS TINYINT) as PRT_LABEL_FLAG",
	"CAST(o_SLOT_UNUSABLE AS TINYINT) as SLOT_UNUSABLE_FLAG",
	"CAST(LEN AS DECIMAL(16,4)) as LEN",
	"CAST(WIDTH AS DECIMAL(16,4)) as WIDTH",
	"CAST(HT AS DECIMAL(16,4)) as HT",
	"CAST(X_COORD AS DECIMAL(13,5)) as X_COORD",
	"CAST(Y_COORD AS DECIMAL(13,5)) as Y_COORD",
	"CAST(Z_COORD AS DECIMAL(13,5)) as Z_COORD",
	"CAST(AREA AS STRING) as AREA",
	"CAST(ZONE AS STRING) as ZONE",
	"CAST(AISLE AS STRING) as AISLE",
	"CAST(BAY AS STRING) as BAY",
	"CAST(LVL AS STRING) as LVL",
	"CAST(POSN AS STRING) as POSN",
	"CAST(TRAVEL_AISLE AS STRING) as TRAVEL_AISLE",
	"CAST(TRAVEL_ZONE AS STRING) as TRAVEL_ZONE",
	"CAST(STORAGE_UOM AS STRING) as WM_STORAGE_UOM",
	"CAST(PICK_UOM AS STRING) as WM_PICK_UOM",
	"CAST(USER_ID AS STRING) as WM_USER_ID",
	"CAST(WM_VERSION_ID AS INT) as WM_VERSION_ID",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)


try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LOCN_HDR_ID = target.WM_LOCN_HDR_ID"""
#   refined_perf_table = "WM_LOCN_HDR"
  executeMerge(Shortcut_to_WM_LOCN_HDR1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LOCN_HDR", "WM_LOCN_HDR", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LOCN_HDR", "WM_LOCN_HDR","Failed",str(e), f"{raw}.log_run_details", )
  raise e

	