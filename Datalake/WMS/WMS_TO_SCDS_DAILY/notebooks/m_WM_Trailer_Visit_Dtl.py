#Code converted on 2023-06-22 21:02:48
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
refined_perf_table = f"{refine}.WM_TRAILER_VISIT_DTL"
raw_perf_table = f"{raw}.WM_TRAILER_VISIT_DETAIL_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_VISIT_DTL, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_TRAILER_VISIT_DTL = spark.sql(f"""SELECT
LOCATION_ID,
WM_VISIT_DETAIL_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_VISIT_DETAIL_ID IN (SELECT VISIT_DETAIL_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 19

SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE = spark.sql(f"""SELECT
DC_NBR,
VISIT_DETAIL_ID,
VISIT_ID,
TYPE,
APPOINTMENT_ID,
DRIVER_ID,
TRACTOR_ID,
SEAL_NUMBER,
FOB_INDICATOR,
START_DTTM,
END_DTTM,
CREATED_DTTM,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
LAST_UPDATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
PROTECTION_LEVEL,
PRODUCT_CLASS
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_DATA_TYPE_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE_temp = SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE.toDF(*["SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___" + col for col in SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE.columns])

EXP_DATA_TYPE_CONVERSION = SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___VISIT_ID as VISIT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___TYPE as TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___APPOINTMENT_ID as APPOINTMENT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___DRIVER_ID as DRIVER_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___TRACTOR_ID as TRACTOR_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___SEAL_NUMBER as SEAL_NUMBER", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___FOB_INDICATOR as FOB_INDICATOR", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___START_DTTM as START_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___END_DTTM as END_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DETAIL_PRE___PRODUCT_CLASS as PRODUCT_CLASS" 
)

# COMMAND ----------
# Processing node JNR_SITE, type JOINER 
# COLUMN COUNT: 21

JNR_SITE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_DATA_TYPE_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_DATA_TYPE_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_SRC_TGT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_temp = JNR_SITE.toDF(*["JNR_SITE___" + col for col in JNR_SITE.columns])
SQ_Shortcut_to_WM_TRAILER_VISIT_DTL_temp = SQ_Shortcut_to_WM_TRAILER_VISIT_DTL.toDF(*["SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___" + col for col in SQ_Shortcut_to_WM_TRAILER_VISIT_DTL.columns])

JNR_SRC_TGT = SQ_Shortcut_to_WM_TRAILER_VISIT_DTL_temp.join(JNR_SITE_temp,[SQ_Shortcut_to_WM_TRAILER_VISIT_DTL_temp.SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___LOCATION_ID == JNR_SITE_temp.JNR_SITE___LOCATION_ID, SQ_Shortcut_to_WM_TRAILER_VISIT_DTL_temp.SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___WM_VISIT_DETAIL_ID == JNR_SITE_temp.JNR_SITE___VISIT_DETAIL_ID],'right_outer').selectExpr( 
	"JNR_SITE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"JNR_SITE___VISIT_ID as VISIT_ID", 
	"JNR_SITE___TYPE as TYPE", 
	"JNR_SITE___APPOINTMENT_ID as APPOINTMENT_ID", 
	"JNR_SITE___DRIVER_ID as DRIVER_ID", 
	"JNR_SITE___TRACTOR_ID as TRACTOR_ID", 
	"JNR_SITE___SEAL_NUMBER as SEAL_NUMBER", 
	"JNR_SITE___FOB_INDICATOR as FOB_INDICATOR", 
	"JNR_SITE___START_DTTM as START_DTTM", 
	"JNR_SITE___END_DTTM as END_DTTM", 
	"JNR_SITE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SITE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SITE___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"JNR_SITE___PRODUCT_CLASS as PRODUCT_CLASS", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___LOCATION_ID as TGT_LOCATION_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___WM_VISIT_DETAIL_ID as WM_VISIT_DETAIL_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_DTL___LOAD_TSTMP as TGT_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
JNR_SRC_TGT_temp = JNR_SRC_TGT.toDF(*["JNR_SRC_TGT___" + col for col in JNR_SRC_TGT.columns])

FIL_UNCHANGED_RECORDS = JNR_SRC_TGT_temp.selectExpr( 
	"JNR_SRC_TGT___LOCATION_ID as LOCATION_ID", 
	"JNR_SRC_TGT___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"JNR_SRC_TGT___VISIT_ID as VISIT_ID", 
	"JNR_SRC_TGT___TYPE as TYPE", 
	"JNR_SRC_TGT___APPOINTMENT_ID as APPOINTMENT_ID", 
	"JNR_SRC_TGT___DRIVER_ID as DRIVER_ID", 
	"JNR_SRC_TGT___TRACTOR_ID as TRACTOR_ID", 
	"JNR_SRC_TGT___SEAL_NUMBER as SEAL_NUMBER", 
	"JNR_SRC_TGT___FOB_INDICATOR as FOB_INDICATOR", 
	"JNR_SRC_TGT___START_DTTM as START_DTTM", 
	"JNR_SRC_TGT___END_DTTM as END_DTTM", 
	"JNR_SRC_TGT___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SRC_TGT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SRC_TGT___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SRC_TGT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SRC_TGT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SRC_TGT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SRC_TGT___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"JNR_SRC_TGT___PRODUCT_CLASS as PRODUCT_CLASS", 
	"JNR_SRC_TGT___WM_VISIT_DETAIL_ID as WM_VISIT_DETAIL_ID", 
	"JNR_SRC_TGT___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"JNR_SRC_TGT___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"JNR_SRC_TGT___TGT_LOAD_TSTMP as TGT_LOAD_TSTMP").filter(expr("WM_VISIT_DETAIL_ID IS NULL OR (NOT WM_VISIT_DETAIL_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"FIL_UNCHANGED_RECORDS___VISIT_ID as VISIT_ID", 
	"FIL_UNCHANGED_RECORDS___TYPE as TYPE", 
	"FIL_UNCHANGED_RECORDS___APPOINTMENT_ID as APPOINTMENT_ID", 
	"FIL_UNCHANGED_RECORDS___DRIVER_ID as DRIVER_ID", 
	"FIL_UNCHANGED_RECORDS___TRACTOR_ID as TRACTOR_ID", 
	"FIL_UNCHANGED_RECORDS___SEAL_NUMBER as SEAL_NUMBER", 
	"FIL_UNCHANGED_RECORDS___FOB_INDICATOR as FOB_INDICATOR", 
	"FIL_UNCHANGED_RECORDS___START_DTTM as START_DTTM", 
	"FIL_UNCHANGED_RECORDS___END_DTTM as END_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"FIL_UNCHANGED_RECORDS___PRODUCT_CLASS as PRODUCT_CLASS", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___TGT_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___TGT_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___WM_VISIT_DETAIL_ID IS NULL, 1, 2) as UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
EXP_VALIDATOR_temp = EXP_VALIDATOR.toDF(*["EXP_VALIDATOR___" + col for col in EXP_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_VALIDATOR_temp.selectExpr( 
	"EXP_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_VALIDATOR___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"EXP_VALIDATOR___VISIT_ID as VISIT_ID", 
	"EXP_VALIDATOR___TYPE as TYPE", 
	"EXP_VALIDATOR___APPOINTMENT_ID as APPOINTMENT_ID", 
	"EXP_VALIDATOR___DRIVER_ID as DRIVER_ID", 
	"EXP_VALIDATOR___TRACTOR_ID as TRACTOR_ID", 
	"EXP_VALIDATOR___SEAL_NUMBER as SEAL_NUMBER", 
	"EXP_VALIDATOR___FOB_INDICATOR as FOB_INDICATOR", 
	"EXP_VALIDATOR___START_DTTM as START_DTTM", 
	"EXP_VALIDATOR___END_DTTM as END_DTTM", 
	"EXP_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"EXP_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"EXP_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_VALIDATOR___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"EXP_VALIDATOR___PRODUCT_CLASS as PRODUCT_CLASS", 
	"EXP_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_VALIDATOR___UPDATE_VALIDATOR as UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_VALIDATOR.UPDATE_VALIDATOR ==(lit(1)),lit(0)).when(EXP_VALIDATOR.UPDATE_VALIDATOR ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_TRAILER_VISIT_DTL1, type TARGET 
# COLUMN COUNT: 21


Shortcut_to_WM_TRAILER_VISIT_DTL1 = UPD_INSERT_UPDATE.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(VISIT_DETAIL_ID AS BIGINT) as WM_VISIT_DETAIL_ID", 
	"CAST(VISIT_ID AS BIGINT) as WM_VISIT_ID", 
	"CAST(TYPE AS STRING) as WM_VISIT_TYPE", 
	"CAST(APPOINTMENT_ID AS BIGINT) as WM_APPOINTMENT_ID", 
	"CAST(DRIVER_ID AS BIGINT) as WM_DRIVER_ID", 
	"CAST(TRACTOR_ID AS BIGINT) as WM_TRACTOR_ID", 
	"CAST(SEAL_NUMBER AS STRING) as WM_SEAL_NUMBER", 
	"CAST(FOB_INDICATOR AS BIGINT) as WM_FOB_IND", 
	"CAST(START_DTTM AS TIMESTAMP) as WM_TRAILER_START_TSTMP", 
	"CAST(END_DTTM AS TIMESTAMP) as WM_TRAILER_END_TSTMP", 
	"CAST(PROTECTION_LEVEL AS STRING) as WM_PROTECTION_LEVEL", 
	"CAST(PRODUCT_CLASS AS STRING) as WM_PRODUCT_CLASS", 
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP", 
	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as WM_CREATED_SOURCE_TYPE", 
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE", 
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", 
	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE", 
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)


try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_VISIT_DETAIL_ID = target.WM_VISIT_DETAIL_ID"""
  # refined_perf_table = "WM_TRAILER_VISIT_DTL"
  executeMerge(Shortcut_to_WM_TRAILER_VISIT_DTL1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_TRAILER_VISIT_DTL", "WM_TRAILER_VISIT_DTL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_TRAILER_VISIT_DTL", "WM_TRAILER_VISIT_DTL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	