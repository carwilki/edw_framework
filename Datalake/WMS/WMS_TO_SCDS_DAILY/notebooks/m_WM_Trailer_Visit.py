#Code converted on 2023-06-22 21:02:49
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
# Processing node SQ_Shortcut_to_WM_TRAILER_VISIT, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_TRAILER_VISIT = spark.sql(f"""SELECT
WM_TRAILER_VISIT.LOCATION_ID,
WM_TRAILER_VISIT.WM_VISIT_ID,
WM_TRAILER_VISIT.WM_CREATED_TSTMP,
WM_TRAILER_VISIT.WM_LAST_UPDATED_TSTMP,
WM_TRAILER_VISIT.LOAD_TSTMP
FROM WM_TRAILER_VISIT
WHERE WM_VISIT_ID in (SELECT VISIT_ID FROM WM_TRAILER_VISIT_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_VISIT_PRE, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_WM_TRAILER_VISIT_PRE = spark.sql(f"""SELECT
WM_TRAILER_VISIT_PRE.DC_NBR,
WM_TRAILER_VISIT_PRE.VISIT_ID,
WM_TRAILER_VISIT_PRE.FACILITY_ID,
WM_TRAILER_VISIT_PRE.TRAILER_ID,
WM_TRAILER_VISIT_PRE.CHECKIN_DTTM,
WM_TRAILER_VISIT_PRE.CHECKOUT_DTTM,
WM_TRAILER_VISIT_PRE.CREATED_DTTM,
WM_TRAILER_VISIT_PRE.CREATED_SOURCE_TYPE,
WM_TRAILER_VISIT_PRE.CREATED_SOURCE,
WM_TRAILER_VISIT_PRE.LAST_UPDATED_DTTM,
WM_TRAILER_VISIT_PRE.LAST_UPDATED_SOURCE_TYPE,
WM_TRAILER_VISIT_PRE.LAST_UPDATED_SOURCE
FROM WM_TRAILER_VISIT_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_DATA_TYPE_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_TRAILER_VISIT_PRE_temp = SQ_Shortcut_to_WM_TRAILER_VISIT_PRE.toDF(*["SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___" + col for col in SQ_Shortcut_to_WM_TRAILER_VISIT_PRE.columns])

EXP_DATA_TYPE_CONVERSION = SQ_Shortcut_to_WM_TRAILER_VISIT_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___VISIT_ID as VISIT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___FACILITY_ID as FACILITY_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___TRAILER_ID as TRAILER_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___CHECKIN_DTTM as CHECKIN_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___CHECKOUT_DTTM as CHECKOUT_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE" 
)

# COMMAND ----------
# Processing node JNR_SITE, type JOINER 
# COLUMN COUNT: 14

JNR_SITE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_DATA_TYPE_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_DATA_TYPE_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_SRC_TGT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_temp = JNR_SITE.toDF(*["JNR_SITE___" + col for col in JNR_SITE.columns])
SQ_Shortcut_to_WM_TRAILER_VISIT_temp = SQ_Shortcut_to_WM_TRAILER_VISIT.toDF(*["SQ_Shortcut_to_WM_TRAILER_VISIT___" + col for col in SQ_Shortcut_to_WM_TRAILER_VISIT.columns])

JNR_SRC_TGT = SQ_Shortcut_to_WM_TRAILER_VISIT_temp.join(JNR_SITE_temp,[SQ_Shortcut_to_WM_TRAILER_VISIT_temp.SQ_Shortcut_to_WM_TRAILER_VISIT___LOCATION_ID == JNR_SITE_temp.JNR_SITE___LOCATION_ID, SQ_Shortcut_to_WM_TRAILER_VISIT_temp.SQ_Shortcut_to_WM_TRAILER_VISIT___WM_VISIT_ID == JNR_SITE_temp.JNR_SITE___VISIT_ID],'right_outer').selectExpr( 
	"JNR_SITE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE___VISIT_ID as VISIT_ID", 
	"JNR_SITE___FACILITY_ID as FACILITY_ID", 
	"JNR_SITE___TRAILER_ID as TRAILER_ID", 
	"JNR_SITE___CHECKIN_DTTM as CHECKIN_DTTM", 
	"JNR_SITE___CHECKOUT_DTTM as CHECKOUT_DTTM", 
	"JNR_SITE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SITE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT___LOCATION_ID as LOCATION_ID1", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT___WM_VISIT_ID as WM_VISIT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_VISIT___LOAD_TSTMP as LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_SRC_TGT_temp = JNR_SRC_TGT.toDF(*["JNR_SRC_TGT___" + col for col in JNR_SRC_TGT.columns])

FIL_UNCHANGED_RECORDS = JNR_SRC_TGT_temp.selectExpr( 
	"JNR_SRC_TGT___LOCATION_ID as LOCATION_ID", 
	"JNR_SRC_TGT___VISIT_ID as VISIT_ID", 
	"JNR_SRC_TGT___FACILITY_ID as FACILITY_ID", 
	"JNR_SRC_TGT___TRAILER_ID as TRAILER_ID", 
	"JNR_SRC_TGT___CHECKIN_DTTM as CHECKIN_DTTM", 
	"JNR_SRC_TGT___CHECKOUT_DTTM as CHECKOUT_DTTM", 
	"JNR_SRC_TGT___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SRC_TGT___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SRC_TGT___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SRC_TGT___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SRC_TGT___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SRC_TGT___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SRC_TGT___LOCATION_ID1 as TGT_LOCATION_ID", 
	"JNR_SRC_TGT___WM_VISIT_ID as WM_VISIT_ID", 
	"JNR_SRC_TGT___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"JNR_SRC_TGT___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"JNR_SRC_TGT___LOAD_TSTMP as LOAD_TSTMP").filter(expr("WM_VISIT_ID IS NULL OR (NOT WM_VISIT_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_VALIDATOR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___WM_VISIT_ID as WM_VISIT_ID", 
	"FIL_UNCHANGED_RECORDS___LOAD_TSTMP as TGT_LOAD_TSTMP", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___VISIT_ID as VISIT_ID", 
	"FIL_UNCHANGED_RECORDS___FACILITY_ID as FACILITY_ID", 
	"FIL_UNCHANGED_RECORDS___TRAILER_ID as TRAILER_ID", 
	"FIL_UNCHANGED_RECORDS___CHECKIN_DTTM as CHECKIN_DTTM", 
	"FIL_UNCHANGED_RECORDS___CHECKOUT_DTTM as CHECKOUT_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE").selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___VISIT_ID as VISIT_ID", 
	"FIL_UNCHANGED_RECORDS___FACILITY_ID as FACILITY_ID", 
	"FIL_UNCHANGED_RECORDS___TRAILER_ID as TRAILER_ID", 
	"FIL_UNCHANGED_RECORDS___CHECKIN_DTTM as CHECKIN_DTTM", 
	"FIL_UNCHANGED_RECORDS___CHECKOUT_DTTM as CHECKOUT_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___TGT_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___TGT_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___WM_VISIT_ID IS NULL, 1, 2) as UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
EXP_VALIDATOR_temp = EXP_VALIDATOR.toDF(*["EXP_VALIDATOR___" + col for col in EXP_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_VALIDATOR_temp.selectExpr( 
	"EXP_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_VALIDATOR___VISIT_ID as VISIT_ID", 
	"EXP_VALIDATOR___FACILITY_ID as FACILITY_ID", 
	"EXP_VALIDATOR___TRAILER_ID as TRAILER_ID", 
	"EXP_VALIDATOR___CHECKIN_DTTM as CHECKIN_DTTM", 
	"EXP_VALIDATOR___CHECKOUT_DTTM as CHECKOUT_DTTM", 
	"EXP_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"EXP_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"EXP_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_VALIDATOR___UPDATE_VALIDATOR as UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_VALIDATOR.UPDATE_VALIDATOR ==(lit(1)) , lit(0)).when(EXP_VALIDATOR.UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_TRAILER_VISIT, type TARGET 
# COLUMN COUNT: 14

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_VISIT_ID = target.WM_VISIT_ID"""
  refined_perf_table = "WM_TRAILER_VISIT"
  executeMerge(UPD_INSERT_UPDATE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_TRAILER_VISIT", "WM_TRAILER_VISIT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_TRAILER_VISIT", "WM_TRAILER_VISIT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	