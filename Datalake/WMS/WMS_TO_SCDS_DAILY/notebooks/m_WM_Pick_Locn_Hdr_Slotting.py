#Code converted on 2023-06-26 17:06:11
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
refined_perf_table = f"{refine}.WM_PICK_LOCN_HDR_SLOTTING"
raw_perf_table = f"{raw}.WM_PICK_LOCN_HDR_SLOTTING_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING, type SOURCE 
# COLUMN COUNT: 49

SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING = spark.sql(f"""SELECT
LOCATION_ID,
WM_PICK_LOCN_HDR_ID,
WM_SLOTTING_GROUP,
WM_SLOT_PRIORITY,
WM_RACK_LEVEL_ID,
WM_RACK_TYPE,
MY_RANGE,
MY_SNS,
SIDE_OF_AISLE,
LABEL_POS,
LABEL_POS_OVR,
DEPTH_OVERRIDE,
HT_OVERRIDE,
WIDTH_OVERRIDE,
WT_LIMIT_OVERRIDE,
OLD_REC_SLOT_WIDTH,
RIGHT_SLOT,
LEFT_SLOT,
REACH_DIST,
REACH_DIST_OVERRIDE,
ALLOW_EXPAND_FLAG,
ALLOW_EXPAND_LFT_FLAG,
ALLOW_EXPAND_LFT_OVR_FLAG,
ALLOW_EXPAND_OVR_FLAG,
ALLOW_EXPAND_RGT_FLAG,
ALLOW_EXPAND_RGT_OVR_FLAG,
MAX_HC_OVR,
MAX_HT_CLEAR,
MAX_LANE_WT,
MAX_LANES,
MAX_LN_OVR,
MAX_LW_OVR,
MAX_SC_OVR,
MAX_SIDE_CLEAR,
MAX_ST_OVR,
MAX_STACK,
LOCKED_FLAG,
PROCESSED_FLAG,
RESERVED_1,
RESERVED_2,
RESERVED_3,
RESERVED_4,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_PICK_LOCN_HDR_ID IN (SELECT PICK_LOCN_HDR_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE, type SOURCE 
# COLUMN COUNT: 49

SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE = spark.sql(f"""SELECT
DC_NBR,
PICK_LOCN_HDR_ID,
ALLOW_EXPAND,
ALLOW_EXPAND_LFT,
ALLOW_EXPAND_LFT_OVR,
ALLOW_EXPAND_OVR,
ALLOW_EXPAND_RGT,
ALLOW_EXPAND_RGT_OVR,
DEPTH_OVERRIDE,
HT_OVERRIDE,
LABEL_POS,
LABEL_POS_OVR,
LEFT_SLOT,
LOCKED,
MAX_HC_OVR,
MAX_HT_CLEAR,
MAX_LANE_WT,
MAX_LANES,
MAX_LN_OVR,
MAX_LW_OVR,
MAX_SC_OVR,
MAX_SIDE_CLEAR,
MAX_ST_OVR,
MAX_STACK,
MY_RANGE,
MY_SNS,
OLD_REC_SLOT_WIDTH,
PROCESSED,
RACK_LEVEL_ID,
RACK_TYPE,
REACH_DIST,
REACH_DIST_OVERRIDE,
RESERVED_1,
RESERVED_2,
RESERVED_3,
RESERVED_4,
RIGHT_SLOT,
SIDE_OF_AISLE,
SLOT_PRIORITY,
WIDTH_OVERRIDE,
WT_LIMIT_OVERRIDE,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM,
SLOTTING_GROUP,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 49

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE_temp = SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE.toDF(*["SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___" + col for col in SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___ALLOW_EXPAND as ALLOW_EXPAND", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___ALLOW_EXPAND_LFT as ALLOW_EXPAND_LFT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___ALLOW_EXPAND_LFT_OVR as ALLOW_EXPAND_LFT_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___ALLOW_EXPAND_OVR as ALLOW_EXPAND_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___ALLOW_EXPAND_RGT as ALLOW_EXPAND_RGT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___ALLOW_EXPAND_RGT_OVR as ALLOW_EXPAND_RGT_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___DEPTH_OVERRIDE as DEPTH_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___HT_OVERRIDE as HT_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LABEL_POS as LABEL_POS", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LABEL_POS_OVR as LABEL_POS_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LEFT_SLOT as LEFT_SLOT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LOCKED as LOCKED", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_HC_OVR as MAX_HC_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_HT_CLEAR as MAX_HT_CLEAR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_LANE_WT as MAX_LANE_WT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_LANES as MAX_LANES", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_LN_OVR as MAX_LN_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_LW_OVR as MAX_LW_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_SC_OVR as MAX_SC_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_SIDE_CLEAR as MAX_SIDE_CLEAR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_ST_OVR as MAX_ST_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MAX_STACK as MAX_STACK", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MY_RANGE as MY_RANGE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___MY_SNS as MY_SNS", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___OLD_REC_SLOT_WIDTH as OLD_REC_SLOT_WIDTH", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___PROCESSED as PROCESSED", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RACK_LEVEL_ID as RACK_LEVEL_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RACK_TYPE as RACK_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___REACH_DIST as REACH_DIST", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___REACH_DIST_OVERRIDE as REACH_DIST_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RESERVED_1 as RESERVED_1", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RESERVED_2 as RESERVED_2", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RESERVED_3 as RESERVED_3", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RESERVED_4 as RESERVED_4", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___RIGHT_SLOT as RIGHT_SLOT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___SIDE_OF_AISLE as SIDE_OF_AISLE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___SLOT_PRIORITY as SLOT_PRIORITY", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___WIDTH_OVERRIDE as WIDTH_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___WT_LIMIT_OVERRIDE as WT_LIMIT_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___SLOTTING_GROUP as SLOTTING_GROUP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 51

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_PICK_LOCN_HDR_SLOTTING, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 98

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_temp = SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING.toDF(*["SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___" + col for col in SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING.columns])

JNR_WM_PICK_LOCN_HDR_SLOTTING = SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_temp.SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING_temp.SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_PICK_LOCN_HDR_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PICK_LOCN_HDR_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
	"JNR_SITE_PROFILE___ALLOW_EXPAND as ALLOW_EXPAND", \
	"JNR_SITE_PROFILE___ALLOW_EXPAND_LFT as ALLOW_EXPAND_LFT", \
	"JNR_SITE_PROFILE___ALLOW_EXPAND_LFT_OVR as ALLOW_EXPAND_LFT_OVR", \
	"JNR_SITE_PROFILE___ALLOW_EXPAND_OVR as ALLOW_EXPAND_OVR", \
	"JNR_SITE_PROFILE___ALLOW_EXPAND_RGT as ALLOW_EXPAND_RGT", \
	"JNR_SITE_PROFILE___ALLOW_EXPAND_RGT_OVR as ALLOW_EXPAND_RGT_OVR", \
	"JNR_SITE_PROFILE___DEPTH_OVERRIDE as DEPTH_OVERRIDE", \
	"JNR_SITE_PROFILE___HT_OVERRIDE as HT_OVERRIDE", \
	"JNR_SITE_PROFILE___LABEL_POS as LABEL_POS", \
	"JNR_SITE_PROFILE___LABEL_POS_OVR as LABEL_POS_OVR", \
	"JNR_SITE_PROFILE___LEFT_SLOT as LEFT_SLOT", \
	"JNR_SITE_PROFILE___LOCKED as LOCKED", \
	"JNR_SITE_PROFILE___MAX_HC_OVR as MAX_HC_OVR", \
	"JNR_SITE_PROFILE___MAX_HT_CLEAR as MAX_HT_CLEAR", \
	"JNR_SITE_PROFILE___MAX_LANE_WT as MAX_LANE_WT", \
	"JNR_SITE_PROFILE___MAX_LANES as MAX_LANES", \
	"JNR_SITE_PROFILE___MAX_LN_OVR as MAX_LN_OVR", \
	"JNR_SITE_PROFILE___MAX_LW_OVR as MAX_LW_OVR", \
	"JNR_SITE_PROFILE___MAX_SC_OVR as MAX_SC_OVR", \
	"JNR_SITE_PROFILE___MAX_SIDE_CLEAR as MAX_SIDE_CLEAR", \
	"JNR_SITE_PROFILE___MAX_ST_OVR as MAX_ST_OVR", \
	"JNR_SITE_PROFILE___MAX_STACK as MAX_STACK", \
	"JNR_SITE_PROFILE___MY_RANGE as MY_RANGE", \
	"JNR_SITE_PROFILE___MY_SNS as MY_SNS", \
	"JNR_SITE_PROFILE___OLD_REC_SLOT_WIDTH as OLD_REC_SLOT_WIDTH", \
	"JNR_SITE_PROFILE___PROCESSED as PROCESSED", \
	"JNR_SITE_PROFILE___RACK_LEVEL_ID as RACK_LEVEL_ID", \
	"JNR_SITE_PROFILE___RACK_TYPE as RACK_TYPE", \
	"JNR_SITE_PROFILE___REACH_DIST as REACH_DIST", \
	"JNR_SITE_PROFILE___REACH_DIST_OVERRIDE as REACH_DIST_OVERRIDE", \
	"JNR_SITE_PROFILE___RESERVED_1 as RESERVED_1", \
	"JNR_SITE_PROFILE___RESERVED_2 as RESERVED_2", \
	"JNR_SITE_PROFILE___RESERVED_3 as RESERVED_3", \
	"JNR_SITE_PROFILE___RESERVED_4 as RESERVED_4", \
	"JNR_SITE_PROFILE___RIGHT_SLOT as RIGHT_SLOT", \
	"JNR_SITE_PROFILE___SIDE_OF_AISLE as SIDE_OF_AISLE", \
	"JNR_SITE_PROFILE___SLOT_PRIORITY as SLOT_PRIORITY", \
	"JNR_SITE_PROFILE___WIDTH_OVERRIDE as WIDTH_OVERRIDE", \
	"JNR_SITE_PROFILE___WT_LIMIT_OVERRIDE as WT_LIMIT_OVERRIDE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___SLOTTING_GROUP as SLOTTING_GROUP", \
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_PICK_LOCN_HDR_ID as WM_PICK_LOCN_HDR_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_SLOTTING_GROUP as WM_SLOTTING_GROUP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_SLOT_PRIORITY as WM_SLOT_PRIORITY", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_RACK_LEVEL_ID as WM_RACK_LEVEL_ID", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_RACK_TYPE as WM_RACK_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MY_RANGE as in_MY_RANGE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MY_SNS as in_MY_SNS", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___SIDE_OF_AISLE as in_SIDE_OF_AISLE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LABEL_POS as in_LABEL_POS", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LABEL_POS_OVR as in_LABEL_POS_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___DEPTH_OVERRIDE as in_DEPTH_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___HT_OVERRIDE as in_HT_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WIDTH_OVERRIDE as in_WIDTH_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WT_LIMIT_OVERRIDE as in_WT_LIMIT_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___OLD_REC_SLOT_WIDTH as in_OLD_REC_SLOT_WIDTH", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___RIGHT_SLOT as in_RIGHT_SLOT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LEFT_SLOT as in_LEFT_SLOT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___REACH_DIST as in_REACH_DIST", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___REACH_DIST_OVERRIDE as in_REACH_DIST_OVERRIDE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_FLAG as ALLOW_EXPAND_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT_FLAG as ALLOW_EXPAND_LFT_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT_OVR_FLAG as ALLOW_EXPAND_LFT_OVR_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_OVR_FLAG as ALLOW_EXPAND_OVR_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT_FLAG as ALLOW_EXPAND_RGT_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT_OVR_FLAG as ALLOW_EXPAND_RGT_OVR_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_HC_OVR as in_MAX_HC_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_HT_CLEAR as in_MAX_HT_CLEAR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_LANE_WT as in_MAX_LANE_WT", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_LANES as in_MAX_LANES", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_LN_OVR as in_MAX_LN_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_LW_OVR as in_MAX_LW_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_SC_OVR as in_MAX_SC_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_SIDE_CLEAR as in_MAX_SIDE_CLEAR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_ST_OVR as in_MAX_ST_OVR", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___MAX_STACK as in_MAX_STACK", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LOCKED_FLAG as LOCKED_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___PROCESSED_FLAG as PROCESSED_FLAG", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_1 as in_RESERVED_1", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_2 as in_RESERVED_2", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_3 as in_RESERVED_3", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_4 as in_RESERVED_4", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 98

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_PICK_LOCN_HDR_SLOTTING_temp = JNR_WM_PICK_LOCN_HDR_SLOTTING.toDF(*["JNR_WM_PICK_LOCN_HDR_SLOTTING___" + col for col in JNR_WM_PICK_LOCN_HDR_SLOTTING.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_PICK_LOCN_HDR_SLOTTING_temp.selectExpr( \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND as ALLOW_EXPAND", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT as ALLOW_EXPAND_LFT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT_OVR as ALLOW_EXPAND_LFT_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_OVR as ALLOW_EXPAND_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT as ALLOW_EXPAND_RGT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT_OVR as ALLOW_EXPAND_RGT_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___DEPTH_OVERRIDE as DEPTH_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___HT_OVERRIDE as HT_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LABEL_POS as LABEL_POS", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LABEL_POS_OVR as LABEL_POS_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LEFT_SLOT as LEFT_SLOT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LOCKED as LOCKED", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_HC_OVR as MAX_HC_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_HT_CLEAR as MAX_HT_CLEAR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_LANE_WT as MAX_LANE_WT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_LANES as MAX_LANES", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_LN_OVR as MAX_LN_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_LW_OVR as MAX_LW_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_SC_OVR as MAX_SC_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_SIDE_CLEAR as MAX_SIDE_CLEAR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_ST_OVR as MAX_ST_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MAX_STACK as MAX_STACK", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MY_RANGE as MY_RANGE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___MY_SNS as MY_SNS", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___OLD_REC_SLOT_WIDTH as OLD_REC_SLOT_WIDTH", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___PROCESSED as PROCESSED", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RACK_LEVEL_ID as RACK_LEVEL_ID", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RACK_TYPE as RACK_TYPE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___REACH_DIST as REACH_DIST", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___REACH_DIST_OVERRIDE as REACH_DIST_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_1 as RESERVED_1", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_2 as RESERVED_2", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_3 as RESERVED_3", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RESERVED_4 as RESERVED_4", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___RIGHT_SLOT as RIGHT_SLOT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___SIDE_OF_AISLE as SIDE_OF_AISLE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___SLOT_PRIORITY as SLOT_PRIORITY", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WIDTH_OVERRIDE as WIDTH_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WT_LIMIT_OVERRIDE as WT_LIMIT_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___SLOTTING_GROUP as SLOTTING_GROUP", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_PICK_LOCN_HDR_ID as WM_PICK_LOCN_HDR_ID", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_SLOTTING_GROUP as WM_SLOTTING_GROUP", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_SLOT_PRIORITY as WM_SLOT_PRIORITY", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_RACK_LEVEL_ID as WM_RACK_LEVEL_ID", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_RACK_TYPE as WM_RACK_TYPE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MY_RANGE as in_MY_RANGE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MY_SNS as in_MY_SNS", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_SIDE_OF_AISLE as in_SIDE_OF_AISLE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_LABEL_POS as in_LABEL_POS", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_LABEL_POS_OVR as in_LABEL_POS_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_DEPTH_OVERRIDE as in_DEPTH_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_HT_OVERRIDE as in_HT_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_WIDTH_OVERRIDE as in_WIDTH_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_WT_LIMIT_OVERRIDE as in_WT_LIMIT_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_OLD_REC_SLOT_WIDTH as in_OLD_REC_SLOT_WIDTH", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_RIGHT_SLOT as in_RIGHT_SLOT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_LEFT_SLOT as in_LEFT_SLOT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_REACH_DIST as in_REACH_DIST", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_REACH_DIST_OVERRIDE as in_REACH_DIST_OVERRIDE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_FLAG as ALLOW_EXPAND_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT_FLAG as ALLOW_EXPAND_LFT_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_LFT_OVR_FLAG as ALLOW_EXPAND_LFT_OVR_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_OVR_FLAG as ALLOW_EXPAND_OVR_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT_FLAG as ALLOW_EXPAND_RGT_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___ALLOW_EXPAND_RGT_OVR_FLAG as ALLOW_EXPAND_RGT_OVR_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_HC_OVR as in_MAX_HC_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_HT_CLEAR as in_MAX_HT_CLEAR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_LANE_WT as in_MAX_LANE_WT", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_LANES as in_MAX_LANES", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_LN_OVR as in_MAX_LN_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_LW_OVR as in_MAX_LW_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_SC_OVR as in_MAX_SC_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_SIDE_CLEAR as in_MAX_SIDE_CLEAR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_ST_OVR as in_MAX_ST_OVR", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_MAX_STACK as in_MAX_STACK", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___LOCKED_FLAG as LOCKED_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___PROCESSED_FLAG as PROCESSED_FLAG", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_RESERVED_1 as in_RESERVED_1", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_RESERVED_2 as in_RESERVED_2", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_RESERVED_3 as in_RESERVED_3", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_RESERVED_4 as in_RESERVED_4", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_PICK_LOCN_HDR_SLOTTING___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_PICK_LOCN_HDR_ID is Null OR (  WM_PICK_LOCN_HDR_ID is NOT Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 101

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND as ALLOW_EXPAND", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_LFT as ALLOW_EXPAND_LFT", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_LFT_OVR as ALLOW_EXPAND_LFT_OVR", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_OVR as ALLOW_EXPAND_OVR", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_RGT as ALLOW_EXPAND_RGT", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_RGT_OVR as ALLOW_EXPAND_RGT_OVR", \
	"FIL_UNCHANGED_RECORDS___DEPTH_OVERRIDE as DEPTH_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___HT_OVERRIDE as HT_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___LABEL_POS as LABEL_POS", \
	"FIL_UNCHANGED_RECORDS___LABEL_POS_OVR as LABEL_POS_OVR", \
	"FIL_UNCHANGED_RECORDS___LEFT_SLOT as LEFT_SLOT", \
	"FIL_UNCHANGED_RECORDS___LOCKED as LOCKED", \
	"FIL_UNCHANGED_RECORDS___MAX_HC_OVR as MAX_HC_OVR", \
	"FIL_UNCHANGED_RECORDS___MAX_HT_CLEAR as MAX_HT_CLEAR", \
	"FIL_UNCHANGED_RECORDS___MAX_LANE_WT as MAX_LANE_WT", \
	"FIL_UNCHANGED_RECORDS___MAX_LANES as MAX_LANES", \
	"FIL_UNCHANGED_RECORDS___MAX_LN_OVR as MAX_LN_OVR", \
	"FIL_UNCHANGED_RECORDS___MAX_LW_OVR as MAX_LW_OVR", \
	"FIL_UNCHANGED_RECORDS___MAX_SC_OVR as MAX_SC_OVR", \
	"FIL_UNCHANGED_RECORDS___MAX_SIDE_CLEAR as MAX_SIDE_CLEAR", \
	"FIL_UNCHANGED_RECORDS___MAX_ST_OVR as MAX_ST_OVR", \
	"FIL_UNCHANGED_RECORDS___MAX_STACK as MAX_STACK", \
	"FIL_UNCHANGED_RECORDS___MY_RANGE as MY_RANGE", \
	"FIL_UNCHANGED_RECORDS___MY_SNS as MY_SNS", \
	"FIL_UNCHANGED_RECORDS___OLD_REC_SLOT_WIDTH as OLD_REC_SLOT_WIDTH", \
	"FIL_UNCHANGED_RECORDS___PROCESSED as PROCESSED", \
	"FIL_UNCHANGED_RECORDS___RACK_LEVEL_ID as RACK_LEVEL_ID", \
	"FIL_UNCHANGED_RECORDS___RACK_TYPE as RACK_TYPE", \
	"FIL_UNCHANGED_RECORDS___REACH_DIST as REACH_DIST", \
	"FIL_UNCHANGED_RECORDS___REACH_DIST_OVERRIDE as REACH_DIST_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___RESERVED_1 as RESERVED_1", \
	"FIL_UNCHANGED_RECORDS___RESERVED_2 as RESERVED_2", \
	"FIL_UNCHANGED_RECORDS___RESERVED_3 as RESERVED_3", \
	"FIL_UNCHANGED_RECORDS___RESERVED_4 as RESERVED_4", \
	"FIL_UNCHANGED_RECORDS___RIGHT_SLOT as RIGHT_SLOT", \
	"FIL_UNCHANGED_RECORDS___SIDE_OF_AISLE as SIDE_OF_AISLE", \
	"FIL_UNCHANGED_RECORDS___SLOT_PRIORITY as SLOT_PRIORITY", \
	"FIL_UNCHANGED_RECORDS___WIDTH_OVERRIDE as WIDTH_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___WT_LIMIT_OVERRIDE as WT_LIMIT_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___SLOTTING_GROUP as SLOTTING_GROUP", \
	"FIL_UNCHANGED_RECORDS___LOAD_TSTMP as LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___WM_PICK_LOCN_HDR_ID as WM_PICK_LOCN_HDR_ID", \
	"FIL_UNCHANGED_RECORDS___WM_SLOTTING_GROUP as WM_SLOTTING_GROUP", \
	"FIL_UNCHANGED_RECORDS___WM_SLOT_PRIORITY as WM_SLOT_PRIORITY", \
	"FIL_UNCHANGED_RECORDS___WM_RACK_LEVEL_ID as WM_RACK_LEVEL_ID", \
	"FIL_UNCHANGED_RECORDS___WM_RACK_TYPE as WM_RACK_TYPE", \
	"FIL_UNCHANGED_RECORDS___in_MY_RANGE as in_MY_RANGE", \
	"FIL_UNCHANGED_RECORDS___in_MY_SNS as in_MY_SNS", \
	"FIL_UNCHANGED_RECORDS___in_SIDE_OF_AISLE as in_SIDE_OF_AISLE", \
	"FIL_UNCHANGED_RECORDS___in_LABEL_POS as in_LABEL_POS", \
	"FIL_UNCHANGED_RECORDS___in_LABEL_POS_OVR as in_LABEL_POS_OVR", \
	"FIL_UNCHANGED_RECORDS___in_DEPTH_OVERRIDE as in_DEPTH_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___in_HT_OVERRIDE as in_HT_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___in_WIDTH_OVERRIDE as in_WIDTH_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___in_WT_LIMIT_OVERRIDE as in_WT_LIMIT_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___in_OLD_REC_SLOT_WIDTH as in_OLD_REC_SLOT_WIDTH", \
	"FIL_UNCHANGED_RECORDS___in_RIGHT_SLOT as in_RIGHT_SLOT", \
	"FIL_UNCHANGED_RECORDS___in_LEFT_SLOT as in_LEFT_SLOT", \
	"FIL_UNCHANGED_RECORDS___in_REACH_DIST as in_REACH_DIST", \
	"FIL_UNCHANGED_RECORDS___in_REACH_DIST_OVERRIDE as in_REACH_DIST_OVERRIDE", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_FLAG as ALLOW_EXPAND_FLAG", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_LFT_FLAG as ALLOW_EXPAND_LFT_FLAG", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_LFT_OVR_FLAG as ALLOW_EXPAND_LFT_OVR_FLAG", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_OVR_FLAG as ALLOW_EXPAND_OVR_FLAG", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_RGT_FLAG as ALLOW_EXPAND_RGT_FLAG", \
	"FIL_UNCHANGED_RECORDS___ALLOW_EXPAND_RGT_OVR_FLAG as ALLOW_EXPAND_RGT_OVR_FLAG", \
	"FIL_UNCHANGED_RECORDS___in_MAX_HC_OVR as in_MAX_HC_OVR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_HT_CLEAR as in_MAX_HT_CLEAR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_LANE_WT as in_MAX_LANE_WT", \
	"FIL_UNCHANGED_RECORDS___in_MAX_LANES as in_MAX_LANES", \
	"FIL_UNCHANGED_RECORDS___in_MAX_LN_OVR as in_MAX_LN_OVR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_LW_OVR as in_MAX_LW_OVR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_SC_OVR as in_MAX_SC_OVR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_SIDE_CLEAR as in_MAX_SIDE_CLEAR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_ST_OVR as in_MAX_ST_OVR", \
	"FIL_UNCHANGED_RECORDS___in_MAX_STACK as in_MAX_STACK", \
	"FIL_UNCHANGED_RECORDS___LOCKED_FLAG as LOCKED_FLAG", \
	"FIL_UNCHANGED_RECORDS___PROCESSED_FLAG as PROCESSED_FLAG", \
	"FIL_UNCHANGED_RECORDS___in_RESERVED_1 as in_RESERVED_1", \
	"FIL_UNCHANGED_RECORDS___in_RESERVED_2 as in_RESERVED_2", \
	"FIL_UNCHANGED_RECORDS___in_RESERVED_3 as in_RESERVED_3", \
	"FIL_UNCHANGED_RECORDS___in_RESERVED_4 as in_RESERVED_4", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP) as LOAD_TSTMP_EXP", \
	"IF(FIL_UNCHANGED_RECORDS___WM_PICK_LOCN_HDR_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 51

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___PICK_LOCN_HDR_ID as PICK_LOCN_HDR_ID", \
	"EXP_UPD_VALIDATOR___SLOTTING_GROUP as SLOTTING_GROUP", \
	"EXP_UPD_VALIDATOR___SLOT_PRIORITY as SLOT_PRIORITY", \
	"EXP_UPD_VALIDATOR___RACK_LEVEL_ID as RACK_LEVEL_ID", \
	"EXP_UPD_VALIDATOR___RACK_TYPE as RACK_TYPE", \
	"EXP_UPD_VALIDATOR___MY_RANGE as MY_RANGE", \
	"EXP_UPD_VALIDATOR___MY_SNS as MY_SNS", \
	"EXP_UPD_VALIDATOR___SIDE_OF_AISLE as SIDE_OF_AISLE", \
	"EXP_UPD_VALIDATOR___LABEL_POS as LABEL_POS", \
	"EXP_UPD_VALIDATOR___LABEL_POS_OVR as LABEL_POS_OVR", \
	"EXP_UPD_VALIDATOR___DEPTH_OVERRIDE as DEPTH_OVERRIDE", \
	"EXP_UPD_VALIDATOR___HT_OVERRIDE as HT_OVERRIDE", \
	"EXP_UPD_VALIDATOR___WIDTH_OVERRIDE as WIDTH_OVERRIDE", \
	"EXP_UPD_VALIDATOR___WT_LIMIT_OVERRIDE as WT_LIMIT_OVERRIDE", \
	"EXP_UPD_VALIDATOR___OLD_REC_SLOT_WIDTH as OLD_REC_SLOT_WIDTH", \
	"EXP_UPD_VALIDATOR___RIGHT_SLOT as RIGHT_SLOT", \
	"EXP_UPD_VALIDATOR___LEFT_SLOT as LEFT_SLOT", \
	"EXP_UPD_VALIDATOR___REACH_DIST as REACH_DIST", \
	"EXP_UPD_VALIDATOR___REACH_DIST_OVERRIDE as REACH_DIST_OVERRIDE", \
	"EXP_UPD_VALIDATOR___ALLOW_EXPAND as ALLOW_EXPAND", \
	"EXP_UPD_VALIDATOR___ALLOW_EXPAND_LFT as ALLOW_EXPAND_LFT", \
	"EXP_UPD_VALIDATOR___ALLOW_EXPAND_LFT_OVR as ALLOW_EXPAND_LFT_OVR", \
	"EXP_UPD_VALIDATOR___ALLOW_EXPAND_OVR as ALLOW_EXPAND_OVR", \
	"EXP_UPD_VALIDATOR___ALLOW_EXPAND_RGT as ALLOW_EXPAND_RGT", \
	"EXP_UPD_VALIDATOR___ALLOW_EXPAND_RGT_OVR as ALLOW_EXPAND_RGT_OVR", \
	"EXP_UPD_VALIDATOR___MAX_HC_OVR as MAX_HC_OVR", \
	"EXP_UPD_VALIDATOR___MAX_HT_CLEAR as MAX_HT_CLEAR", \
	"EXP_UPD_VALIDATOR___MAX_LANE_WT as MAX_LANE_WT", \
	"EXP_UPD_VALIDATOR___MAX_LANES as MAX_LANES", \
	"EXP_UPD_VALIDATOR___MAX_LN_OVR as MAX_LN_OVR", \
	"EXP_UPD_VALIDATOR___MAX_LW_OVR as MAX_LW_OVR", \
	"EXP_UPD_VALIDATOR___MAX_SC_OVR as MAX_SC_OVR", \
	"EXP_UPD_VALIDATOR___MAX_SIDE_CLEAR as MAX_SIDE_CLEAR", \
	"EXP_UPD_VALIDATOR___MAX_ST_OVR as MAX_ST_OVR", \
	"EXP_UPD_VALIDATOR___MAX_STACK as MAX_STACK", \
	"EXP_UPD_VALIDATOR___LOCKED as LOCKED", \
	"EXP_UPD_VALIDATOR___PROCESSED as PROCESSED", \
	"EXP_UPD_VALIDATOR___RESERVED_1 as RESERVED_1", \
	"EXP_UPD_VALIDATOR___RESERVED_2 as RESERVED_2", \
	"EXP_UPD_VALIDATOR___RESERVED_3 as RESERVED_3", \
	"EXP_UPD_VALIDATOR___RESERVED_4 as RESERVED_4", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP_EXP as LOAD_TSTMP_EXP", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPD_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING, type TARGET 
# COLUMN COUNT: 50


Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(PICK_LOCN_HDR_ID AS BIGINT) as WM_PICK_LOCN_HDR_ID",
	"CAST(SLOTTING_GROUP AS STRING) as WM_SLOTTING_GROUP",
	"CAST(SLOT_PRIORITY AS STRING) as WM_SLOT_PRIORITY",
	"CAST(RACK_LEVEL_ID AS BIGINT) as WM_RACK_LEVEL_ID",
	"CAST(RACK_TYPE AS BIGINT) as WM_RACK_TYPE",
	"CAST(MY_RANGE AS BIGINT) as MY_RANGE",
	"CAST(MY_SNS AS BIGINT) as MY_SNS",
	"CAST(SIDE_OF_AISLE AS SMALLINT) as SIDE_OF_AISLE",
	"CAST(LABEL_POS AS DECIMAL(9,4)) as LABEL_POS",
	"CAST(LABEL_POS_OVR AS TINYINT) as LABEL_POS_OVR",
	"CAST(DEPTH_OVERRIDE AS TINYINT) as DEPTH_OVERRIDE",
	"CAST(HT_OVERRIDE AS TINYINT) as HT_OVERRIDE",
	"CAST(WIDTH_OVERRIDE AS TINYINT) as WIDTH_OVERRIDE",
	"CAST(WT_LIMIT_OVERRIDE AS TINYINT) as WT_LIMIT_OVERRIDE",
	"CAST(OLD_REC_SLOT_WIDTH AS DECIMAL(13,4)) as OLD_REC_SLOT_WIDTH",
	"CAST(RIGHT_SLOT AS BIGINT) as RIGHT_SLOT",
	"CAST(LEFT_SLOT AS BIGINT) as LEFT_SLOT",
	"CAST(REACH_DIST AS DECIMAL(13,4)) as REACH_DIST",
	"CAST(REACH_DIST_OVERRIDE AS TINYINT) as REACH_DIST_OVERRIDE",
	"CAST(ALLOW_EXPAND AS TINYINT) as ALLOW_EXPAND_FLAG",
	"CAST(ALLOW_EXPAND_LFT AS TINYINT) as ALLOW_EXPAND_LFT_FLAG",
	"CAST(ALLOW_EXPAND_LFT_OVR AS TINYINT) as ALLOW_EXPAND_LFT_OVR_FLAG",
	"CAST(ALLOW_EXPAND_OVR AS TINYINT) as ALLOW_EXPAND_OVR_FLAG",
	"CAST(ALLOW_EXPAND_RGT AS TINYINT) as ALLOW_EXPAND_RGT_FLAG",
	"CAST(ALLOW_EXPAND_RGT_OVR AS TINYINT) as ALLOW_EXPAND_RGT_OVR_FLAG",
	"CAST(MAX_HC_OVR AS TINYINT) as MAX_HC_OVR",
	"CAST(MAX_HT_CLEAR AS DECIMAL(9,4)) as MAX_HT_CLEAR",
	"CAST(MAX_LANE_WT AS DECIMAL(13,4)) as MAX_LANE_WT",
	"CAST(MAX_LANES AS BIGINT) as MAX_LANES",
	"CAST(MAX_LN_OVR AS TINYINT) as MAX_LN_OVR",
	"CAST(MAX_LW_OVR AS DECIMAL(9,4)) as MAX_LW_OVR",
	"CAST(MAX_SC_OVR AS TINYINT) as MAX_SC_OVR",
	"CAST(MAX_SIDE_CLEAR AS DECIMAL(9,4)) as MAX_SIDE_CLEAR",
	"CAST(MAX_ST_OVR AS TINYINT) as MAX_ST_OVR",
	"CAST(MAX_STACK AS BIGINT) as MAX_STACK",
	"CAST(LOCKED AS TINYINT) as LOCKED_FLAG",
	"CAST(PROCESSED AS TINYINT) as PROCESSED_FLAG",
	"CAST(RESERVED_1 AS STRING) as RESERVED_1",
	"CAST(RESERVED_2 AS STRING) as RESERVED_2",
	"CAST(RESERVED_3 AS BIGINT) as RESERVED_3",
	"CAST(RESERVED_4 AS BIGINT) as RESERVED_4",
	"CAST(CREATED_SOURCE_TYPE AS TINYINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP", \
    "pyspark_data_action"\
)


try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_PICK_LOCN_HDR_ID = target.WM_PICK_LOCN_HDR_ID"""
  # refined_perf_table = "WM_PICK_LOCN_HDR_SLOTTING"
  executeMerge(Shortcut_to_WM_PICK_LOCN_HDR_SLOTTING, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_PICK_LOCN_HDR_SLOTTING", "WM_PICK_LOCN_HDR_SLOTTING", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_PICK_LOCN_HDR_SLOTTING", "WM_PICK_LOCN_HDR_SLOTTING","Failed",str(e), f"{raw}.log_run_details", )
  raise e

	