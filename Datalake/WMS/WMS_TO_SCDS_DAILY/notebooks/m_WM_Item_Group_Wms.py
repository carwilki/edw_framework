#Code converted on 2023-06-22 10:47:28
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env
env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_ITEM_GROUP_WMS"
raw_perf_table = f"{raw}.WM_ITEM_GROUP_WMS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ITEM_GROUP_WMS, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_ITEM_GROUP_WMS = spark.sql(f"""SELECT
LOCATION_ID,
WM_ITEM_GROUP_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_ITEM_GROUP_ID IN
(SELECT ITEM_GROUP_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE, type SOURCE 
# COLUMN COUNT: 14

SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE = spark.sql(f"""SELECT
DC_NBR,
ITEM_GROUP_ID,
ITEM_ID,
GROUP_TYPE,
GROUP_CODE,
GROUP_ATTRIBUTE,
AUDIT_CREATED_SOURCE_TYPE,
AUDIT_CREATED_DTTM,
AUDIT_LAST_UPDATED_SOURCE_TYPE,
AUDIT_LAST_UPDATED_DTTM,
MARK_FOR_DELETION,
AUDIT_CREATED_SOURCE,
AUDIT_LAST_UPDATED_SOURCE,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE_temp = SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE.toDF(*["SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___" + col for col in SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___DC_NBR as in_DC_NBR", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___ITEM_GROUP_ID as ITEM_GROUP_ID", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___GROUP_TYPE as GROUP_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___GROUP_CODE as GROUP_CODE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___LOAD_TSTMP as LOAD_TSTMP").selectExpr( \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___in_DC_NBR as int) as DC_NBR", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___ITEM_GROUP_ID as ITEM_GROUP_ID", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___GROUP_TYPE as GROUP_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___GROUP_CODE as GROUP_CODE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 16

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_ITEM_GROUP_WMS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_ITEM_GROUP_WMS_temp = SQ_Shortcut_to_WM_ITEM_GROUP_WMS.toDF(*["SQ_Shortcut_to_WM_ITEM_GROUP_WMS___" + col for col in SQ_Shortcut_to_WM_ITEM_GROUP_WMS.columns])

JNR_WM_ITEM_GROUP_WMS = SQ_Shortcut_to_WM_ITEM_GROUP_WMS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ITEM_GROUP_WMS_temp.SQ_Shortcut_to_WM_ITEM_GROUP_WMS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_ITEM_GROUP_WMS_temp.SQ_Shortcut_to_WM_ITEM_GROUP_WMS___WM_ITEM_GROUP_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ITEM_GROUP_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ITEM_GROUP_ID as ITEM_GROUP_ID", \
	"JNR_SITE_PROFILE___ITEM_ID as ITEM_ID", \
	"JNR_SITE_PROFILE___GROUP_TYPE as GROUP_TYPE", \
	"JNR_SITE_PROFILE___GROUP_CODE as GROUP_CODE", \
	"JNR_SITE_PROFILE___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
	"JNR_SITE_PROFILE___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
	"JNR_SITE_PROFILE___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"JNR_SITE_PROFILE___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
	"JNR_SITE_PROFILE___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS___WM_ITEM_GROUP_ID as in_WM_ITEM_GROUP_ID", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS___LOAD_TSTMP as in_LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_ITEM_GROUP_WMS___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ITEM_GROUP_WMS_temp = JNR_WM_ITEM_GROUP_WMS.toDF(*["JNR_WM_ITEM_GROUP_WMS___" + col for col in JNR_WM_ITEM_GROUP_WMS.columns])

FIL_NO_CHANGE_REC = JNR_WM_ITEM_GROUP_WMS_temp.selectExpr( \
	"JNR_WM_ITEM_GROUP_WMS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ITEM_GROUP_WMS___ITEM_GROUP_ID as ITEM_GROUP_ID", \
	"JNR_WM_ITEM_GROUP_WMS___ITEM_ID as ITEM_ID", \
	"JNR_WM_ITEM_GROUP_WMS___GROUP_TYPE as GROUP_TYPE", \
	"JNR_WM_ITEM_GROUP_WMS___GROUP_CODE as GROUP_CODE", \
	"JNR_WM_ITEM_GROUP_WMS___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
	"JNR_WM_ITEM_GROUP_WMS___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
	"JNR_WM_ITEM_GROUP_WMS___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
	"JNR_WM_ITEM_GROUP_WMS___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_ITEM_GROUP_WMS___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
	"JNR_WM_ITEM_GROUP_WMS___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"JNR_WM_ITEM_GROUP_WMS___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
	"JNR_WM_ITEM_GROUP_WMS___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
	"JNR_WM_ITEM_GROUP_WMS___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_ITEM_GROUP_WMS___in_WM_ITEM_GROUP_ID as in_WM_ITEM_GROUP_ID", \
	"JNR_WM_ITEM_GROUP_WMS___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"JNR_WM_ITEM_GROUP_WMS___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_ITEM_GROUP_WMS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_ITEM_GROUP_WMS___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP") \
	.filter("in_WM_ITEM_GROUP_ID is Null OR ( in_WM_ITEM_GROUP_ID is not Null and ( COALEASE(AUDIT_CREATED_DTTM, date'1900-01-01') != COALEASE(WM_CREATED_TSTMP, date'1900-01-01') OR \
            COALEASE(AUDIT_LAST_UPDATED_DTTM, date'1900-01-01') != COALEASE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___ITEM_GROUP_ID as ITEM_GROUP_ID", \
	"FIL_NO_CHANGE_REC___ITEM_ID as ITEM_ID", \
	"FIL_NO_CHANGE_REC___GROUP_TYPE as GROUP_TYPE", \
	"FIL_NO_CHANGE_REC___GROUP_CODE as GROUP_CODE", \
	"FIL_NO_CHANGE_REC___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
	"FIL_NO_CHANGE_REC___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
	"FIL_NO_CHANGE_REC___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
	"FIL_NO_CHANGE_REC___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
	"FIL_NO_CHANGE_REC___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
	"FIL_NO_CHANGE_REC___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"FIL_NO_CHANGE_REC___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
	"FIL_NO_CHANGE_REC___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
	"IF(FIL_NO_CHANGE_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_ITEM_GROUP_ID as in_WM_ITEM_GROUP_ID" \
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr( \
	"EXP_EVAL_VALUES___LOCATION_ID as LOCATION_ID", \
	"EXP_EVAL_VALUES___ITEM_GROUP_ID as ITEM_GROUP_ID", \
	"EXP_EVAL_VALUES___ITEM_ID as ITEM_ID", \
	"EXP_EVAL_VALUES___GROUP_TYPE as GROUP_TYPE", \
	"EXP_EVAL_VALUES___GROUP_CODE as GROUP_CODE", \
	"EXP_EVAL_VALUES___GROUP_ATTRIBUTE as GROUP_ATTRIBUTE", \
	"EXP_EVAL_VALUES___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", \
	"EXP_EVAL_VALUES___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", \
	"EXP_EVAL_VALUES___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", \
	"EXP_EVAL_VALUES___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", \
	"EXP_EVAL_VALUES___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"EXP_EVAL_VALUES___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", \
	"EXP_EVAL_VALUES___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", \
	"EXP_EVAL_VALUES___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVAL_VALUES___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_EVAL_VALUES___in_WM_ITEM_GROUP_ID as in_WM_ITEM_GROUP_ID") \
	.withColumn('pyspark_data_action', when((col('in_WM_ITEM_GROUP_ID').isNull()) ,(lit(0))).otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ITEM_GROUP_WMS, type TARGET 
# COLUMN COUNT: 15

Shortcut_to_WM_ITEM_GROUP_WMS = UPD_VALIDATE.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(ITEM_GROUP_ID AS DECIMAL(8,0)) as WM_ITEM_GROUP_ID",
	"CAST(ITEM_ID AS DECIMAL(9,0)) as WM_ITEM_ID",
	"CAST(GROUP_TYPE AS STRING) as WM_GROUP_TYPE",
	"CAST(GROUP_CODE AS STRING) as WM_GROUP_CD",
	"CAST(GROUP_ATTRIBUTE AS STRING) as WM_GROUP_ATTRIBUTE",
	"CAST(MARK_FOR_DELETION AS DECIMAL(1,0)) as MARK_FOR_DELETION_FLAG",
	"CAST(AUDIT_CREATED_SOURCE_TYPE AS DECIMAL(2,0)) as WM_CREATED_SOURCE_TYPE",
	"CAST(AUDIT_CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE",
	"CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS DECIMAL(2,0)) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ITEM_GROUP_ID = target.WM_ITEM_GROUP_ID"""
#   refined_perf_table = "WM_ITEM_GROUP_WMS"
  executeMerge(Shortcut_to_WM_ITEM_GROUP_WMS, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ITEM_GROUP_WMS", "WM_ITEM_GROUP_WMS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ITEM_GROUP_WMS", "WM_ITEM_GROUP_WMS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	