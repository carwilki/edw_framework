#Code converted on 2023-06-22 21:04:40
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
env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_YARD_ZONE_SLOT"
raw_perf_table = f"{raw}.WM_YARD_ZONE_SLOT_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic= ' -- ' # args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE, type SOURCE 
# COLUMN COUNT: 21

SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE = spark.sql(f"""SELECT
DC_NBR,
YARD_ID,
YARD_ZONE_ID,
YARD_ZONE_SLOT_ID,
YARD_ZONE_SLOT_NAME,
YARD_ZONE_SLOT_STATUS,
X_COORDINATE,
Y_COORDINATE,
Z_COORDINATE,
MAX_CAPACITY,
USED_CAPACITY,
MARK_FOR_DELETION,
IS_GUARD_HOUSE,
IS_THRESHOLD_GUARD_HOUSE,
CREATED_DTTM,
LAST_UPDATED_DTTM,
LOCN_ID,
CREATED_SOURCE,
CREATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_SOURCE_TYPE
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_YARD_ZONE_SLOT, type SOURCE 
# COLUMN COUNT: 23

SQ_Shortcut_to_WM_YARD_ZONE_SLOT = spark.sql(f"""SELECT
LOCATION_ID,
WM_YARD_ID,
WM_YARD_ZONE_ID,
WM_YARD_ZONE_SLOT_ID,
WM_YARD_ZONE_SLOT_NAME,
WM_YARD_ZONE_SLOT_STATUS,
WM_LOCN_ID,
X_COORDINATE,
Y_COORDINATE,
Z_COORDINATE,
MAX_CAPACITY,
USED_CAPACITY,
GUARD_HOUSE_FLAG,
THRESHOLD_GUARD_HOUSE_FLAG,
MARK_FOR_DELETION_FLAG,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
WM_LAST_UPDATED_TSTMP,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG =0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE_temp = SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE.toDF(*["SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___" + col for col in SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___YARD_ID as YARD_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___YARD_ZONE_ID as YARD_ZONE_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___YARD_ZONE_SLOT_ID as YARD_ZONE_SLOT_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___YARD_ZONE_SLOT_NAME as YARD_ZONE_SLOT_NAME", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___YARD_ZONE_SLOT_STATUS as YARD_ZONE_SLOT_STATUS", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___X_COORDINATE as X_COORDINATE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___Y_COORDINATE as Y_COORDINATE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___Z_COORDINATE as Z_COORDINATE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___MAX_CAPACITY as MAX_CAPACITY", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___USED_CAPACITY as USED_CAPACITY", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___IS_GUARD_HOUSE as IS_GUARD_HOUSE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___IS_THRESHOLD_GUARD_HOUSE as IS_THRESHOLD_GUARD_HOUSE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___LOCN_ID as LOCN_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 23

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_YARD_ZONE_SLOT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 44

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_YARD_ZONE_SLOT_temp = SQ_Shortcut_to_WM_YARD_ZONE_SLOT.toDF(*["SQ_Shortcut_to_WM_YARD_ZONE_SLOT___" + col for col in SQ_Shortcut_to_WM_YARD_ZONE_SLOT.columns])

JNR_WM_YARD_ZONE_SLOT = SQ_Shortcut_to_WM_YARD_ZONE_SLOT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_YARD_ZONE_SLOT_temp.SQ_Shortcut_to_WM_YARD_ZONE_SLOT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_YARD_ZONE_SLOT_temp.SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___YARD_ID, SQ_Shortcut_to_WM_YARD_ZONE_SLOT_temp.SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ZONE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___YARD_ZONE_ID, SQ_Shortcut_to_WM_YARD_ZONE_SLOT_temp.SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ZONE_SLOT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___YARD_ZONE_SLOT_ID],'fullouter').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___YARD_ID as YARD_ID", 
	"JNR_SITE_PROFILE___YARD_ZONE_ID as YARD_ZONE_ID", 
	"JNR_SITE_PROFILE___YARD_ZONE_SLOT_ID as YARD_ZONE_SLOT_ID", 
	"JNR_SITE_PROFILE___YARD_ZONE_SLOT_NAME as YARD_ZONE_SLOT_NAME", 
	"JNR_SITE_PROFILE___YARD_ZONE_SLOT_STATUS as YARD_ZONE_SLOT_STATUS", 
	"JNR_SITE_PROFILE___X_COORDINATE as X_COORDINATE", 
	"JNR_SITE_PROFILE___Y_COORDINATE as Y_COORDINATE", 
	"JNR_SITE_PROFILE___Z_COORDINATE as Z_COORDINATE", 
	"JNR_SITE_PROFILE___MAX_CAPACITY as MAX_CAPACITY", 
	"JNR_SITE_PROFILE___USED_CAPACITY as USED_CAPACITY", 
	"JNR_SITE_PROFILE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_SITE_PROFILE___IS_GUARD_HOUSE as IS_GUARD_HOUSE", 
	"JNR_SITE_PROFILE___IS_THRESHOLD_GUARD_HOUSE as IS_THRESHOLD_GUARD_HOUSE", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___LOCN_ID as LOCN_ID", 
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___LOCATION_ID as i_LOCATION_ID1", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ID as i_WM_YARD_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ZONE_ID as i_WM_YARD_ZONE_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ZONE_SLOT_ID as i_WM_YARD_ZONE_SLOT_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ZONE_SLOT_NAME as i_WM_YARD_ZONE_SLOT_NAME", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_YARD_ZONE_SLOT_STATUS as i_WM_YARD_ZONE_SLOT_STATUS", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_LOCN_ID as i_WM_LOCN_ID", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___X_COORDINATE as i_X_COORDINATE1", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___Y_COORDINATE as i_Y_COORDINATE1", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___Z_COORDINATE as i_Z_COORDINATE1", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___MAX_CAPACITY as i_MAX_CAPACITY1", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___USED_CAPACITY as i_USED_CAPACITY1", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___GUARD_HOUSE_FLAG as i_GUARD_HOUSE_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___THRESHOLD_GUARD_HOUSE_FLAG as i_THRESHOLD_GUARD_HOUSE_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___MARK_FOR_DELETION_FLAG as i_MARK_FOR_DELETION_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___DELETE_FLAG as i_DELETE_FLAG", 
	"SQ_Shortcut_to_WM_YARD_ZONE_SLOT___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 44

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_YARD_ZONE_SLOT_temp = JNR_WM_YARD_ZONE_SLOT.toDF(*["JNR_WM_YARD_ZONE_SLOT___" + col for col in JNR_WM_YARD_ZONE_SLOT.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_YARD_ZONE_SLOT_temp.selectExpr( 
	"JNR_WM_YARD_ZONE_SLOT___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_YARD_ZONE_SLOT___YARD_ID as YARD_ID", 
	"JNR_WM_YARD_ZONE_SLOT___YARD_ZONE_ID as YARD_ZONE_ID", 
	"JNR_WM_YARD_ZONE_SLOT___YARD_ZONE_SLOT_ID as YARD_ZONE_SLOT_ID", 
	"JNR_WM_YARD_ZONE_SLOT___YARD_ZONE_SLOT_NAME as YARD_ZONE_SLOT_NAME", 
	"JNR_WM_YARD_ZONE_SLOT___YARD_ZONE_SLOT_STATUS as YARD_ZONE_SLOT_STATUS", 
	"JNR_WM_YARD_ZONE_SLOT___X_COORDINATE as X_COORDINATE", 
	"JNR_WM_YARD_ZONE_SLOT___Y_COORDINATE as Y_COORDINATE", 
	"JNR_WM_YARD_ZONE_SLOT___Z_COORDINATE as Z_COORDINATE", 
	"JNR_WM_YARD_ZONE_SLOT___MAX_CAPACITY as MAX_CAPACITY", 
	"JNR_WM_YARD_ZONE_SLOT___USED_CAPACITY as USED_CAPACITY", 
	"JNR_WM_YARD_ZONE_SLOT___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_WM_YARD_ZONE_SLOT___IS_GUARD_HOUSE as IS_GUARD_HOUSE", 
	"JNR_WM_YARD_ZONE_SLOT___IS_THRESHOLD_GUARD_HOUSE as IS_THRESHOLD_GUARD_HOUSE", 
	"JNR_WM_YARD_ZONE_SLOT___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_YARD_ZONE_SLOT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_YARD_ZONE_SLOT___LOCN_ID as LOCN_ID", 
	"JNR_WM_YARD_ZONE_SLOT___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_WM_YARD_ZONE_SLOT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE_SLOT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_WM_YARD_ZONE_SLOT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE_SLOT___i_LOCATION_ID1 as i_LOCATION_ID1", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_YARD_ID as i_WM_YARD_ID", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_YARD_ZONE_ID as i_WM_YARD_ZONE_ID", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_YARD_ZONE_SLOT_ID as i_WM_YARD_ZONE_SLOT_ID", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_YARD_ZONE_SLOT_NAME as i_WM_YARD_ZONE_SLOT_NAME", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_YARD_ZONE_SLOT_STATUS as i_WM_YARD_ZONE_SLOT_STATUS", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_LOCN_ID as i_WM_LOCN_ID", 
	"JNR_WM_YARD_ZONE_SLOT___i_X_COORDINATE1 as i_X_COORDINATE1", 
	"JNR_WM_YARD_ZONE_SLOT___i_Y_COORDINATE1 as i_Y_COORDINATE1", 
	"JNR_WM_YARD_ZONE_SLOT___i_Z_COORDINATE1 as i_Z_COORDINATE1", 
	"JNR_WM_YARD_ZONE_SLOT___i_MAX_CAPACITY1 as i_MAX_CAPACITY1", 
	"JNR_WM_YARD_ZONE_SLOT___i_USED_CAPACITY1 as i_USED_CAPACITY1", 
	"JNR_WM_YARD_ZONE_SLOT___i_GUARD_HOUSE_FLAG as i_GUARD_HOUSE_FLAG", 
	"JNR_WM_YARD_ZONE_SLOT___i_THRESHOLD_GUARD_HOUSE_FLAG as i_THRESHOLD_GUARD_HOUSE_FLAG", 
	"JNR_WM_YARD_ZONE_SLOT___i_MARK_FOR_DELETION_FLAG as i_MARK_FOR_DELETION_FLAG", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"JNR_WM_YARD_ZONE_SLOT___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_YARD_ZONE_SLOT___i_DELETE_FLAG as i_DELETE_FLAG", 
	"JNR_WM_YARD_ZONE_SLOT___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("YARD_ZONE_SLOT_ID IS NULL OR i_WM_YARD_ZONE_SLOT_ID IS NULL OR (NOT i_WM_YARD_ZONE_SLOT_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 48

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
    .withColumn("FIL_UNCHANGED_RECORDS___v_CREATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___CREATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___CREATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP)"""))

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ID as YARD_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ZONE_ID as YARD_ZONE_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_ID as YARD_ZONE_SLOT_ID", 
	"FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_NAME as YARD_ZONE_SLOT_NAME", 
	"FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_STATUS as YARD_ZONE_SLOT_STATUS", 
	"FIL_UNCHANGED_RECORDS___X_COORDINATE as X_COORDINATE", 
	"FIL_UNCHANGED_RECORDS___Y_COORDINATE as Y_COORDINATE", 
	"FIL_UNCHANGED_RECORDS___Z_COORDINATE as Z_COORDINATE", 
	"FIL_UNCHANGED_RECORDS___MAX_CAPACITY as MAX_CAPACITY", 
	"FIL_UNCHANGED_RECORDS___USED_CAPACITY as USED_CAPACITY", 
	"FIL_UNCHANGED_RECORDS___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"FIL_UNCHANGED_RECORDS___IS_GUARD_HOUSE as IS_GUARD_HOUSE", 
	"FIL_UNCHANGED_RECORDS___IS_THRESHOLD_GUARD_HOUSE as IS_THRESHOLD_GUARD_HOUSE", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LOCN_ID as LOCN_ID", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 as i_LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ID as i_WM_YARD_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_ID as i_WM_YARD_ZONE_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_ID as i_WM_YARD_ZONE_SLOT_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_NAME as i_WM_YARD_ZONE_SLOT_NAME", 
	"FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_STATUS as i_WM_YARD_ZONE_SLOT_STATUS", 
	"FIL_UNCHANGED_RECORDS___i_WM_LOCN_ID as i_WM_LOCN_ID", 
	"FIL_UNCHANGED_RECORDS___i_X_COORDINATE1 as i_X_COORDINATE1", 
	"FIL_UNCHANGED_RECORDS___i_Y_COORDINATE1 as i_Y_COORDINATE1", 
	"FIL_UNCHANGED_RECORDS___i_Z_COORDINATE1 as i_Z_COORDINATE1", 
	"FIL_UNCHANGED_RECORDS___i_MAX_CAPACITY1 as i_MAX_CAPACITY1", 
	"FIL_UNCHANGED_RECORDS___i_USED_CAPACITY1 as i_USED_CAPACITY1", 
	"FIL_UNCHANGED_RECORDS___i_GUARD_HOUSE_FLAG as i_GUARD_HOUSE_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_THRESHOLD_GUARD_HOUSE_FLAG as i_THRESHOLD_GUARD_HOUSE_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_MARK_FOR_DELETION_FLAG as i_MARK_FOR_DELETION_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_DELETE_FLAG as i_DELETE_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_ID IS NOT NULL, 1, 0) as DELETE_FLAG", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	f"IF(FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) ), 'DELETE', IF(FIL_UNCHANGED_RECORDS___YARD_ZONE_SLOT_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_YARD_ZONE_SLOT_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM ), 'UPDATE', NULL))) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node RTR_INS_UPD_DEL, type ROUTER 
# COLUMN COUNT: 48


# Creating output dataframe for RTR_INS_UPD_DEL, output group DELETE
RTR_INS_UPD_DEL_DELETE = EXP_OUTPUT_VALIDATOR.select(EXP_OUTPUT_VALIDATOR.sys_row_id.alias('sys_row_id'), 
	EXP_OUTPUT_VALIDATOR.LOCATION_ID.alias('LOCATION_ID3'), 
	EXP_OUTPUT_VALIDATOR.YARD_ID.alias('YARD_ID3'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_ID.alias('YARD_ZONE_ID3'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_SLOT_ID.alias('YARD_ZONE_SLOT_ID3'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_SLOT_NAME.alias('YARD_ZONE_SLOT_NAME3'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_SLOT_STATUS.alias('YARD_ZONE_SLOT_STATUS3'), 
	EXP_OUTPUT_VALIDATOR.X_COORDINATE.alias('X_COORDINATE3'), 
	EXP_OUTPUT_VALIDATOR.Y_COORDINATE.alias('Y_COORDINATE3'), 
	EXP_OUTPUT_VALIDATOR.Z_COORDINATE.alias('Z_COORDINATE3'), 
	EXP_OUTPUT_VALIDATOR.MAX_CAPACITY.alias('MAX_CAPACITY3'), 
	EXP_OUTPUT_VALIDATOR.USED_CAPACITY.alias('USED_CAPACITY3'), 
	EXP_OUTPUT_VALIDATOR.MARK_FOR_DELETION.alias('MARK_FOR_DELETION3'), 
	EXP_OUTPUT_VALIDATOR.IS_GUARD_HOUSE.alias('IS_GUARD_HOUSE3'), 
	EXP_OUTPUT_VALIDATOR.IS_THRESHOLD_GUARD_HOUSE.alias('IS_THRESHOLD_GUARD_HOUSE3'), 
	EXP_OUTPUT_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM3'), 
	EXP_OUTPUT_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM3'), 
	EXP_OUTPUT_VALIDATOR.LOCN_ID.alias('LOCN_ID3'), 
	EXP_OUTPUT_VALIDATOR.CREATED_SOURCE.alias('CREATED_SOURCE3'), 
	EXP_OUTPUT_VALIDATOR.CREATED_SOURCE_TYPE.alias('CREATED_SOURCE_TYPE3'), 
	EXP_OUTPUT_VALIDATOR.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE3'), 
	EXP_OUTPUT_VALIDATOR.LAST_UPDATED_SOURCE_TYPE.alias('LAST_UPDATED_SOURCE_TYPE3'), 
	EXP_OUTPUT_VALIDATOR.i_LOCATION_ID1.alias('i_LOCATION_ID13'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ID.alias('i_WM_YARD_ID3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_ID.alias('i_WM_YARD_ZONE_ID3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_SLOT_ID.alias('i_WM_YARD_ZONE_SLOT_ID3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_SLOT_NAME.alias('i_WM_YARD_ZONE_SLOT_NAME3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_SLOT_STATUS.alias('i_WM_YARD_ZONE_SLOT_STATUS3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LOCN_ID.alias('i_WM_LOCN_ID3'), 
	EXP_OUTPUT_VALIDATOR.i_X_COORDINATE1.alias('i_X_COORDINATE13'), 
	EXP_OUTPUT_VALIDATOR.i_Y_COORDINATE1.alias('i_Y_COORDINATE13'), 
	EXP_OUTPUT_VALIDATOR.i_Z_COORDINATE1.alias('i_Z_COORDINATE13'), 
	EXP_OUTPUT_VALIDATOR.i_MAX_CAPACITY1.alias('i_MAX_CAPACITY13'), 
	EXP_OUTPUT_VALIDATOR.i_USED_CAPACITY1.alias('i_USED_CAPACITY13'), 
	EXP_OUTPUT_VALIDATOR.i_GUARD_HOUSE_FLAG.alias('i_GUARD_HOUSE_FLAG3'), 
	EXP_OUTPUT_VALIDATOR.i_THRESHOLD_GUARD_HOUSE_FLAG.alias('i_THRESHOLD_GUARD_HOUSE_FLAG3'), 
	EXP_OUTPUT_VALIDATOR.i_MARK_FOR_DELETION_FLAG.alias('i_MARK_FOR_DELETION_FLAG3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_CREATED_SOURCE_TYPE.alias('i_WM_CREATED_SOURCE_TYPE3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_CREATED_SOURCE.alias('i_WM_CREATED_SOURCE3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LAST_UPDATED_SOURCE_TYPE.alias('i_WM_LAST_UPDATED_SOURCE_TYPE3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LAST_UPDATED_SOURCE.alias('i_WM_LAST_UPDATED_SOURCE3'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP3'), 
	EXP_OUTPUT_VALIDATOR.i_DELETE_FLAG.alias('i_DELETE_FLAG3'), 
	EXP_OUTPUT_VALIDATOR.i_LOAD_TSTMP.alias('i_LOAD_TSTMP3'), 
	EXP_OUTPUT_VALIDATOR.DELETE_FLAG.alias('DELETE_FLAG3'), 
	EXP_OUTPUT_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP3'), 
	EXP_OUTPUT_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP3'), 
	EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR3')).filter("o_UPDATE_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_INS_UPD_DEL, output group INSERT_UPDATE
RTR_INS_UPD_DEL_INSERT_UPDATE = EXP_OUTPUT_VALIDATOR.select(EXP_OUTPUT_VALIDATOR.sys_row_id.alias('sys_row_id'), 
	EXP_OUTPUT_VALIDATOR.LOCATION_ID.alias('LOCATION_ID1'), 
	EXP_OUTPUT_VALIDATOR.YARD_ID.alias('YARD_ID1'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_ID.alias('YARD_ZONE_ID1'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_SLOT_ID.alias('YARD_ZONE_SLOT_ID1'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_SLOT_NAME.alias('YARD_ZONE_SLOT_NAME1'), 
	EXP_OUTPUT_VALIDATOR.YARD_ZONE_SLOT_STATUS.alias('YARD_ZONE_SLOT_STATUS1'), 
	EXP_OUTPUT_VALIDATOR.X_COORDINATE.alias('X_COORDINATE1'), 
	EXP_OUTPUT_VALIDATOR.Y_COORDINATE.alias('Y_COORDINATE1'), 
	EXP_OUTPUT_VALIDATOR.Z_COORDINATE.alias('Z_COORDINATE1'), 
	EXP_OUTPUT_VALIDATOR.MAX_CAPACITY.alias('MAX_CAPACITY1'), 
	EXP_OUTPUT_VALIDATOR.USED_CAPACITY.alias('USED_CAPACITY1'), 
	EXP_OUTPUT_VALIDATOR.MARK_FOR_DELETION.alias('MARK_FOR_DELETION1'), 
	EXP_OUTPUT_VALIDATOR.IS_GUARD_HOUSE.alias('IS_GUARD_HOUSE1'), 
	EXP_OUTPUT_VALIDATOR.IS_THRESHOLD_GUARD_HOUSE.alias('IS_THRESHOLD_GUARD_HOUSE1'), 
	EXP_OUTPUT_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM1'), 
	EXP_OUTPUT_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM1'), 
	EXP_OUTPUT_VALIDATOR.LOCN_ID.alias('LOCN_ID1'), 
	EXP_OUTPUT_VALIDATOR.CREATED_SOURCE.alias('CREATED_SOURCE1'), 
	EXP_OUTPUT_VALIDATOR.CREATED_SOURCE_TYPE.alias('CREATED_SOURCE_TYPE1'), 
	EXP_OUTPUT_VALIDATOR.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE1'), 
	EXP_OUTPUT_VALIDATOR.LAST_UPDATED_SOURCE_TYPE.alias('LAST_UPDATED_SOURCE_TYPE1'), 
	EXP_OUTPUT_VALIDATOR.i_LOCATION_ID1.alias('i_LOCATION_ID11'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ID.alias('i_WM_YARD_ID1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_ID.alias('i_WM_YARD_ZONE_ID1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_SLOT_ID.alias('i_WM_YARD_ZONE_SLOT_ID1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_SLOT_NAME.alias('i_WM_YARD_ZONE_SLOT_NAME1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_YARD_ZONE_SLOT_STATUS.alias('i_WM_YARD_ZONE_SLOT_STATUS1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LOCN_ID.alias('i_WM_LOCN_ID1'), 
	EXP_OUTPUT_VALIDATOR.i_X_COORDINATE1.alias('i_X_COORDINATE11'), 
	EXP_OUTPUT_VALIDATOR.i_Y_COORDINATE1.alias('i_Y_COORDINATE11'), 
	EXP_OUTPUT_VALIDATOR.i_Z_COORDINATE1.alias('i_Z_COORDINATE11'), 
	EXP_OUTPUT_VALIDATOR.i_MAX_CAPACITY1.alias('i_MAX_CAPACITY11'), 
	EXP_OUTPUT_VALIDATOR.i_USED_CAPACITY1.alias('i_USED_CAPACITY11'), 
	EXP_OUTPUT_VALIDATOR.i_GUARD_HOUSE_FLAG.alias('i_GUARD_HOUSE_FLAG1'), 
	EXP_OUTPUT_VALIDATOR.i_THRESHOLD_GUARD_HOUSE_FLAG.alias('i_THRESHOLD_GUARD_HOUSE_FLAG1'), 
	EXP_OUTPUT_VALIDATOR.i_MARK_FOR_DELETION_FLAG.alias('i_MARK_FOR_DELETION_FLAG1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_CREATED_SOURCE_TYPE.alias('i_WM_CREATED_SOURCE_TYPE1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_CREATED_SOURCE.alias('i_WM_CREATED_SOURCE1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LAST_UPDATED_SOURCE_TYPE.alias('i_WM_LAST_UPDATED_SOURCE_TYPE1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LAST_UPDATED_SOURCE.alias('i_WM_LAST_UPDATED_SOURCE1'), 
	EXP_OUTPUT_VALIDATOR.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP1'), 
	EXP_OUTPUT_VALIDATOR.i_DELETE_FLAG.alias('i_DELETE_FLAG1'), 
	EXP_OUTPUT_VALIDATOR.i_LOAD_TSTMP.alias('i_LOAD_TSTMP1'), 
	EXP_OUTPUT_VALIDATOR.DELETE_FLAG.alias('DELETE_FLAG1'), 
	EXP_OUTPUT_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP1'), 
	EXP_OUTPUT_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP1'), 
	EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR1')).filter("o_UPDATE_VALIDATOR = 'INSERT' OR o_UPDATE_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 25

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_INSERT_UPDATE_temp = RTR_INS_UPD_DEL_INSERT_UPDATE.toDF(*["RTR_INS_UPD_DEL_INSERT_UPDATE___" + col for col in RTR_INS_UPD_DEL_INSERT_UPDATE.columns])

UPD_INS_UPD = RTR_INS_UPD_DEL_INSERT_UPDATE_temp.selectExpr( 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ID1 as YARD_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ZONE_ID1 as YARD_ZONE_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ZONE_SLOT_ID1 as YARD_ZONE_SLOT_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ZONE_SLOT_NAME1 as YARD_ZONE_SLOT_NAME1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___YARD_ZONE_SLOT_STATUS1 as YARD_ZONE_SLOT_STATUS1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___X_COORDINATE1 as X_COORDINATE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___Y_COORDINATE1 as Y_COORDINATE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___Z_COORDINATE1 as Z_COORDINATE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MAX_CAPACITY1 as MAX_CAPACITY1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___USED_CAPACITY1 as USED_CAPACITY1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___MARK_FOR_DELETION1 as MARK_FOR_DELETION1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___IS_GUARD_HOUSE1 as IS_GUARD_HOUSE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___IS_THRESHOLD_GUARD_HOUSE1 as IS_THRESHOLD_GUARD_HOUSE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_DTTM1 as CREATED_DTTM1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_DTTM1 as LAST_UPDATED_DTTM1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOCN_ID1 as LOCN_ID1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_SOURCE1 as CREATED_SOURCE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___CREATED_SOURCE_TYPE1 as CREATED_SOURCE_TYPE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_SOURCE1 as LAST_UPDATED_SOURCE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LAST_UPDATED_SOURCE_TYPE1 as LAST_UPDATED_SOURCE_TYPE1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___DELETE_FLAG1 as DELETE_FLAG1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP1", 
	"RTR_INS_UPD_DEL_INSERT_UPDATE___o_UPDATE_VALIDATOR1 as o_UPDATE_VALIDATOR1"
).withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR1') ==(lit('INSERT')),lit(0)).when(col('o_UPDATE_VALIDATOR1') ==(lit('UPDATE')),lit(1)))

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
RTR_INS_UPD_DEL_DELETE_temp = RTR_INS_UPD_DEL_DELETE.toDF(*["RTR_INS_UPD_DEL_DELETE___" + col for col in RTR_INS_UPD_DEL_DELETE.columns])

UPD_DELETE = RTR_INS_UPD_DEL_DELETE_temp.selectExpr( 
	"RTR_INS_UPD_DEL_DELETE___i_LOCATION_ID13 as i_LOCATION_ID13", 
	"RTR_INS_UPD_DEL_DELETE___i_WM_YARD_ID3 as i_WM_YARD_ID3", 
	"RTR_INS_UPD_DEL_DELETE___i_WM_YARD_ZONE_ID3 as i_WM_YARD_ZONE_ID3", 
	"RTR_INS_UPD_DEL_DELETE___i_WM_YARD_ZONE_SLOT_ID3 as i_WM_YARD_ZONE_SLOT_ID3", 
	"RTR_INS_UPD_DEL_DELETE___DELETE_FLAG3 as DELETE_FLAG3", 
	"RTR_INS_UPD_DEL_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3"
).withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD_ZONE_SLOT11, type TARGET 
# COLUMN COUNT: 24


# Shortcut_to_WM_YARD_ZONE_SLOT11 = UPD_DELETE.selectExpr( 
# 	"CAST(i_LOCATION_ID13 AS BIGINT) as LOCATION_ID", 
# 	"CAST(i_WM_YARD_ID3 AS BIGINT) as WM_YARD_ID", 
# 	"CAST(i_WM_YARD_ZONE_ID3 AS BIGINT) as WM_YARD_ZONE_ID", 
# 	"CAST(i_WM_YARD_ZONE_SLOT_ID3 AS BIGINT) as WM_YARD_ZONE_SLOT_ID", 
# 	"CAST(NULL AS STRING) as WM_YARD_ZONE_SLOT_NAME", 
# 	"CAST(NULL AS BIGINT) as WM_YARD_ZONE_SLOT_STATUS", 
# 	"CAST(NULL AS STRING) as WM_LOCN_ID", 
# 	"CAST(NULL AS BIGINT) as X_COORDINATE", 
# 	"CAST(NULL AS BIGINT) as Y_COORDINATE", 
# 	"CAST(NULL AS BIGINT) as Z_COORDINATE", 
# 	"CAST(NULL AS BIGINT) as MAX_CAPACITY", 
# 	"CAST(NULL AS BIGINT) as USED_CAPACITY", 
# 	"CAST(NULL AS BIGINT) as GUARD_HOUSE_FLAG", 
# 	"CAST(NULL AS BIGINT) as THRESHOLD_GUARD_HOUSE_FLAG", 
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
# Shortcut_to_WM_YARD_ZONE_SLOT11.write.saveAsTable(f'{raw}.WM_YARD_ZONE_SLOT')

# COMMAND ----------
# Processing node Shortcut_to_WM_YARD_ZONE_SLOT1, type TARGET 
# COLUMN COUNT: 24


Shortcut_to_WM_YARD_ZONE_SLOT1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(YARD_ID1 AS BIGINT) as WM_YARD_ID",
	"CAST(YARD_ZONE_ID1 AS BIGINT) as WM_YARD_ZONE_ID",
	"CAST(YARD_ZONE_SLOT_ID1 AS BIGINT) as WM_YARD_ZONE_SLOT_ID",
	"CAST(YARD_ZONE_SLOT_NAME1 AS STRING) as WM_YARD_ZONE_SLOT_NAME",
	"CAST(YARD_ZONE_SLOT_STATUS1 AS SMALLINT) as WM_YARD_ZONE_SLOT_STATUS",
	"CAST(LOCN_ID1 AS STRING) as WM_LOCN_ID",
	"CAST(X_COORDINATE1 AS DECIMAL(14,3)) as X_COORDINATE",
	"CAST(Y_COORDINATE1 AS DECIMAL(14,3)) as Y_COORDINATE",
	"CAST(Z_COORDINATE1 AS DECIMAL(14,3)) as Z_COORDINATE",
	"CAST(MAX_CAPACITY1 AS INT) as MAX_CAPACITY",
	"CAST(USED_CAPACITY1 AS INT) as USED_CAPACITY",
	"CAST(IS_GUARD_HOUSE1 AS TINYINT) as GUARD_HOUSE_FLAG",
	"CAST(IS_THRESHOLD_GUARD_HOUSE1 AS TINYINT) as THRESHOLD_GUARD_HOUSE_FLAG",
	"CAST(MARK_FOR_DELETION1 AS TINYINT) as MARK_FOR_DELETION_FLAG",
	"CAST(CREATED_SOURCE_TYPE1 AS TINYINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE1 AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM1 AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE1 AS TINYINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE1 AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM1 AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(DELETE_FLAG1 AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

# TODO check the key done manual
try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_YARD_ID = target.WM_YARD_ID AND source.WM_YARD_ZONE_ID = target.WM_YARD_ZONE_ID AND source.WM_YARD_ZONE_SLOT_ID = target.WM_YARD_ZONE_SLOT_ID"""
  # refined_perf_table = "WM_YARD_ZONE_SLOT"
  executeMerge(Shortcut_to_WM_YARD_ZONE_SLOT1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_YARD_ZONE_SLOT", "WM_YARD_ZONE_SLOT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_YARD_ZONE_SLOT", "WM_YARD_ZONE_SLOT","Failed",str(e), f"{raw}.log_run_details", )
  raise e

