#Code converted on 2023-06-26 17:03:42
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
refined_perf_table = f"{refine}.WM_PICK_LOCN_DTL_SLOTTING"
raw_perf_table = f"{raw}.WM_PICK_LOCN_DTL_SLOTTING_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE, type SOURCE 
# COLUMN COUNT: 23

SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE = spark.sql(f"""SELECT
DC_NBR,
PICK_LOCN_DTL_ID,
HIST_MATCH,
CUR_ORIENTATION,
IGN_FOR_RESLOT,
REC_LANES,
REC_STACKING,
HI_RESIDUAL_1,
OPT_PALLET_PATTERN,
SI_NUM_1,
SI_NUM_2,
SI_NUM_3,
SI_NUM_4,
SI_NUM_5,
SI_NUM_6,
MULT_LOC_GRP,
REPLEN_GROUP,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE_temp = SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE.toDF(*["SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___" + col for col in SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___HIST_MATCH as HIST_MATCH", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___CUR_ORIENTATION as CUR_ORIENTATION", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___IGN_FOR_RESLOT as IGN_FOR_RESLOT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___REC_LANES as REC_LANES", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___REC_STACKING as REC_STACKING", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___HI_RESIDUAL_1 as HI_RESIDUAL_1", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___SI_NUM_1 as SI_NUM_1", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___SI_NUM_2 as SI_NUM_2", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___SI_NUM_3 as SI_NUM_3", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___SI_NUM_4 as SI_NUM_4", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___SI_NUM_5 as SI_NUM_5", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___SI_NUM_6 as SI_NUM_6", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___MULT_LOC_GRP as MULT_LOC_GRP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___REPLEN_GROUP as REPLEN_GROUP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING, type SOURCE 
# COLUMN COUNT: 24

SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING = spark.sql(f"""SELECT
LOCATION_ID,
WM_PICK_LOCN_DTL_ID,
REPLEN_GROUP,
CUR_ORIENTATION,
REC_LANES,
REC_STACKING,
MULT_LOC_GRP,
HIST_MATCH,
IGN_FOR_RESLOT_FLAG,
OPT_PALLET_PATTERN,
HI_RESIDUAL_1,
SI_NUM_1,
SI_NUM_2,
SI_NUM_3,
SI_NUM_4,
SI_NUM_5,
SI_NUM_6,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_PICK_LOCN_DTL_ID IN (SELECT PICK_LOCN_DTL_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 25

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_PICK_LOCN_DTL_SLOTTING, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 47

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_temp = SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING.toDF(*["SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___" + col for col in SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_PICK_LOCN_DTL_SLOTTING = SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_temp.SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING_temp.SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_PICK_LOCN_DTL_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PICK_LOCN_DTL_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
	"JNR_SITE_PROFILE___HIST_MATCH as HIST_MATCH", \
	"JNR_SITE_PROFILE___CUR_ORIENTATION as CUR_ORIENTATION", \
	"JNR_SITE_PROFILE___IGN_FOR_RESLOT as IGN_FOR_RESLOT", \
	"JNR_SITE_PROFILE___REC_LANES as REC_LANES", \
	"JNR_SITE_PROFILE___REC_STACKING as REC_STACKING", \
	"JNR_SITE_PROFILE___HI_RESIDUAL_1 as HI_RESIDUAL_1", \
	"JNR_SITE_PROFILE___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", \
	"JNR_SITE_PROFILE___SI_NUM_1 as SI_NUM_1", \
	"JNR_SITE_PROFILE___SI_NUM_2 as SI_NUM_2", \
	"JNR_SITE_PROFILE___SI_NUM_3 as SI_NUM_3", \
	"JNR_SITE_PROFILE___SI_NUM_4 as SI_NUM_4", \
	"JNR_SITE_PROFILE___SI_NUM_5 as SI_NUM_5", \
	"JNR_SITE_PROFILE___SI_NUM_6 as SI_NUM_6", \
	"JNR_SITE_PROFILE___MULT_LOC_GRP as MULT_LOC_GRP", \
	"JNR_SITE_PROFILE___REPLEN_GROUP as REPLEN_GROUP", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_PICK_LOCN_DTL_ID as WM_PICK_LOCN_DTL_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___REPLEN_GROUP as in_REPLEN_GROUP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___CUR_ORIENTATION as in_CUR_ORIENTATION", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___REC_LANES as in_REC_LANES", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___REC_STACKING as in_REC_STACKING", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___MULT_LOC_GRP as in_MULT_LOC_GRP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___HIST_MATCH as in_HIST_MATCH", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___IGN_FOR_RESLOT_FLAG as IGN_FOR_RESLOT_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___OPT_PALLET_PATTERN as in_OPT_PALLET_PATTERN", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___HI_RESIDUAL_1 as in_HI_RESIDUAL_1", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_1 as in_SI_NUM_1", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_2 as in_SI_NUM_2", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_3 as in_SI_NUM_3", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_4 as in_SI_NUM_4", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_5 as in_SI_NUM_5", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_6 as in_SI_NUM_6", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___LOAD_TSTMP as in_LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING___LOCATION_ID as in_LOCATION_ID")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 47

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_PICK_LOCN_DTL_SLOTTING_temp = JNR_WM_PICK_LOCN_DTL_SLOTTING.toDF(*["JNR_WM_PICK_LOCN_DTL_SLOTTING___" + col for col in JNR_WM_PICK_LOCN_DTL_SLOTTING.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_PICK_LOCN_DTL_SLOTTING_temp.selectExpr( \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___HIST_MATCH as HIST_MATCH", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___CUR_ORIENTATION as CUR_ORIENTATION", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___IGN_FOR_RESLOT as IGN_FOR_RESLOT", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___REC_LANES as REC_LANES", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___REC_STACKING as REC_STACKING", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___HI_RESIDUAL_1 as HI_RESIDUAL_1", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_1 as SI_NUM_1", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_2 as SI_NUM_2", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_3 as SI_NUM_3", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_4 as SI_NUM_4", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_5 as SI_NUM_5", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___SI_NUM_6 as SI_NUM_6", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___MULT_LOC_GRP as MULT_LOC_GRP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___REPLEN_GROUP as REPLEN_GROUP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_PICK_LOCN_DTL_ID as WM_PICK_LOCN_DTL_ID", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_REPLEN_GROUP as in_REPLEN_GROUP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_CUR_ORIENTATION as in_CUR_ORIENTATION", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_REC_LANES as in_REC_LANES", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_REC_STACKING as in_REC_STACKING", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_MULT_LOC_GRP as in_MULT_LOC_GRP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_HIST_MATCH as in_HIST_MATCH", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___IGN_FOR_RESLOT_FLAG as IGN_FOR_RESLOT_FLAG", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_OPT_PALLET_PATTERN as in_OPT_PALLET_PATTERN", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_HI_RESIDUAL_1 as in_HI_RESIDUAL_1", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_SI_NUM_1 as in_SI_NUM_1", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_SI_NUM_2 as in_SI_NUM_2", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_SI_NUM_3 as in_SI_NUM_3", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_SI_NUM_4 as in_SI_NUM_4", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_SI_NUM_5 as in_SI_NUM_5", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_SI_NUM_6 as in_SI_NUM_6", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"JNR_WM_PICK_LOCN_DTL_SLOTTING___in_LOCATION_ID as in_LOCATION_ID") \
    .filter("WM_PICK_LOCN_DTL_ID is Null OR (  WM_PICK_LOCN_DTL_ID is NOT Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01') OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 50

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
	"FIL_UNCHANGED_RECORDS___HIST_MATCH as HIST_MATCH", \
	"FIL_UNCHANGED_RECORDS___CUR_ORIENTATION as CUR_ORIENTATION", \
	"FIL_UNCHANGED_RECORDS___IGN_FOR_RESLOT as IGN_FOR_RESLOT", \
	"FIL_UNCHANGED_RECORDS___REC_LANES as REC_LANES", \
	"FIL_UNCHANGED_RECORDS___REC_STACKING as REC_STACKING", \
	"FIL_UNCHANGED_RECORDS___HI_RESIDUAL_1 as HI_RESIDUAL_1", \
	"FIL_UNCHANGED_RECORDS___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", \
	"FIL_UNCHANGED_RECORDS___SI_NUM_1 as SI_NUM_1", \
	"FIL_UNCHANGED_RECORDS___SI_NUM_2 as SI_NUM_2", \
	"FIL_UNCHANGED_RECORDS___SI_NUM_3 as SI_NUM_3", \
	"FIL_UNCHANGED_RECORDS___SI_NUM_4 as SI_NUM_4", \
	"FIL_UNCHANGED_RECORDS___SI_NUM_5 as SI_NUM_5", \
	"FIL_UNCHANGED_RECORDS___SI_NUM_6 as SI_NUM_6", \
	"FIL_UNCHANGED_RECORDS___MULT_LOC_GRP as MULT_LOC_GRP", \
	"FIL_UNCHANGED_RECORDS___REPLEN_GROUP as REPLEN_GROUP", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___WM_PICK_LOCN_DTL_ID as WM_PICK_LOCN_DTL_ID", \
	"FIL_UNCHANGED_RECORDS___in_REPLEN_GROUP as in_REPLEN_GROUP", \
	"FIL_UNCHANGED_RECORDS___in_CUR_ORIENTATION as in_CUR_ORIENTATION", \
	"FIL_UNCHANGED_RECORDS___in_REC_LANES as in_REC_LANES", \
	"FIL_UNCHANGED_RECORDS___in_REC_STACKING as in_REC_STACKING", \
	"FIL_UNCHANGED_RECORDS___in_MULT_LOC_GRP as in_MULT_LOC_GRP", \
	"FIL_UNCHANGED_RECORDS___in_HIST_MATCH as in_HIST_MATCH", \
	"FIL_UNCHANGED_RECORDS___IGN_FOR_RESLOT_FLAG as IGN_FOR_RESLOT_FLAG", \
	"FIL_UNCHANGED_RECORDS___in_OPT_PALLET_PATTERN as in_OPT_PALLET_PATTERN", \
	"FIL_UNCHANGED_RECORDS___in_HI_RESIDUAL_1 as in_HI_RESIDUAL_1", \
	"FIL_UNCHANGED_RECORDS___in_SI_NUM_1 as in_SI_NUM_1", \
	"FIL_UNCHANGED_RECORDS___in_SI_NUM_2 as in_SI_NUM_2", \
	"FIL_UNCHANGED_RECORDS___in_SI_NUM_3 as in_SI_NUM_3", \
	"FIL_UNCHANGED_RECORDS___in_SI_NUM_4 as in_SI_NUM_4", \
	"FIL_UNCHANGED_RECORDS___in_SI_NUM_5 as in_SI_NUM_5", \
	"FIL_UNCHANGED_RECORDS___in_SI_NUM_6 as in_SI_NUM_6", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___in_LOCATION_ID as in_LOCATION_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___WM_PICK_LOCN_DTL_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___PICK_LOCN_DTL_ID as PICK_LOCN_DTL_ID", \
	"EXP_UPD_VALIDATOR___REPLEN_GROUP as REPLEN_GROUP", \
	"EXP_UPD_VALIDATOR___CUR_ORIENTATION as CUR_ORIENTATION", \
	"EXP_UPD_VALIDATOR___REC_LANES as REC_LANES", \
	"EXP_UPD_VALIDATOR___REC_STACKING as REC_STACKING", \
	"EXP_UPD_VALIDATOR___MULT_LOC_GRP as MULT_LOC_GRP", \
	"EXP_UPD_VALIDATOR___HIST_MATCH as HIST_MATCH", \
	"EXP_UPD_VALIDATOR___IGN_FOR_RESLOT as IGN_FOR_RESLOT", \
	"EXP_UPD_VALIDATOR___OPT_PALLET_PATTERN as OPT_PALLET_PATTERN", \
	"EXP_UPD_VALIDATOR___HI_RESIDUAL_1 as HI_RESIDUAL_1", \
	"EXP_UPD_VALIDATOR___SI_NUM_1 as SI_NUM_1", \
	"EXP_UPD_VALIDATOR___SI_NUM_2 as SI_NUM_2", \
	"EXP_UPD_VALIDATOR___SI_NUM_3 as SI_NUM_3", \
	"EXP_UPD_VALIDATOR___SI_NUM_4 as SI_NUM_4", \
	"EXP_UPD_VALIDATOR___SI_NUM_5 as SI_NUM_5", \
	"EXP_UPD_VALIDATOR___SI_NUM_6 as SI_NUM_6", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPD_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING1, type TARGET 
# COLUMN COUNT: 25


Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(PICK_LOCN_DTL_ID AS INT) as WM_PICK_LOCN_DTL_ID",
	"CAST(REPLEN_GROUP AS STRING) as REPLEN_GROUP",
	"CAST(CUR_ORIENTATION AS STRING) as CUR_ORIENTATION",
	"CAST(REC_LANES AS BIGINT) as REC_LANES",
	"CAST(REC_STACKING AS BIGINT) as REC_STACKING",
	"CAST(MULT_LOC_GRP AS STRING) as MULT_LOC_GRP",
	"CAST(HIST_MATCH AS STRING) as HIST_MATCH",
	"CAST(IGN_FOR_RESLOT AS TINYINT) as IGN_FOR_RESLOT_FLAG",
	"CAST(OPT_PALLET_PATTERN AS STRING) as OPT_PALLET_PATTERN",
	"CAST(HI_RESIDUAL_1 AS TINYINT) as HI_RESIDUAL_1",
	"CAST(SI_NUM_1 AS DECIMAL(13,4)) as SI_NUM_1",
	"CAST(SI_NUM_2 AS DECIMAL(13,4)) as SI_NUM_2",
	"CAST(SI_NUM_3 AS DECIMAL(13,4)) as SI_NUM_3",
	"CAST(SI_NUM_4 AS DECIMAL(13,4)) as SI_NUM_4",
	"CAST(SI_NUM_5 AS DECIMAL(13,4)) as SI_NUM_5",
	"CAST(SI_NUM_6 AS DECIMAL(13,4)) as SI_NUM_6",
	"CAST(CREATED_SOURCE_TYPE AS TINYINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action"\
)


try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_PICK_LOCN_DTL_ID = target.WM_PICK_LOCN_DTL_ID"""
  # refined_perf_table = "WM_PICK_LOCN_DTL_SLOTTING"
  executeMerge(Shortcut_to_WM_PICK_LOCN_DTL_SLOTTING1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_PICK_LOCN_DTL_SLOTTING", "WM_PICK_LOCN_DTL_SLOTTING", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_PICK_LOCN_DTL_SLOTTING", "WM_PICK_LOCN_DTL_SLOTTING","Failed",str(e), f"{raw}.log_run_details", )
  raise e

	