#Code converted on 2023-06-22 21:02:53
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
# Processing node SQ_Shortcut_to_WM_TRAILER_REF_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WM_TRAILER_REF_PRE = spark.sql(f"""SELECT
WM_TRAILER_REF_PRE.DC_NBR,
WM_TRAILER_REF_PRE.TRAILER_ID,
WM_TRAILER_REF_PRE.TRAILER_STATUS,
WM_TRAILER_REF_PRE.CURRENT_LOCATION_ID,
WM_TRAILER_REF_PRE.ASSIGNED_LOCATION_ID,
WM_TRAILER_REF_PRE.ACTIVE_VISIT_ID,
WM_TRAILER_REF_PRE.ACTIVE_VISIT_DETAIL_ID,
WM_TRAILER_REF_PRE.CREATED_DTTM,
WM_TRAILER_REF_PRE.CREATED_SOURCE_TYPE,
WM_TRAILER_REF_PRE.CREATED_SOURCE,
WM_TRAILER_REF_PRE.LAST_UPDATED_DTTM,
WM_TRAILER_REF_PRE.LAST_UPDATED_SOURCE_TYPE,
WM_TRAILER_REF_PRE.LAST_UPDATED_SOURCE,
WM_TRAILER_REF_PRE.TRAILER_LOCATION_STATUS,
WM_TRAILER_REF_PRE.CONVEYABLE,
WM_TRAILER_REF_PRE.PROTECTION_LEVEL,
WM_TRAILER_REF_PRE.PRODUCT_CLASS
FROM WM_TRAILER_REF_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_REF, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_TRAILER_REF = spark.sql(f"""SELECT
WM_TRAILER_REF.LOCATION_ID,
WM_TRAILER_REF.WM_TRAILER_ID,
WM_TRAILER_REF.WM_CREATED_TSTMP,
WM_TRAILER_REF.WM_LAST_UPDATED_TSTMP,
WM_TRAILER_REF.LOAD_TSTMP
FROM WM_TRAILER_REF
WHERE WM_TRAILER_ID IN (SELECT TRAILER_ID FROM WM_TRAILER_REF_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_TRAILER_REF_PRE_temp = SQ_Shortcut_to_WM_TRAILER_REF_PRE.toDF(*["SQ_Shortcut_to_WM_TRAILER_REF_PRE___" + col for col in SQ_Shortcut_to_WM_TRAILER_REF_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_TRAILER_REF_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_TRAILER_REF_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___TRAILER_ID as TRAILER_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___TRAILER_STATUS as TRAILER_STATUS", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___CURRENT_LOCATION_ID as CURRENT_LOCATION_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___ASSIGNED_LOCATION_ID as ASSIGNED_LOCATION_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___ACTIVE_VISIT_ID as ACTIVE_VISIT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___ACTIVE_VISIT_DETAIL_ID as ACTIVE_VISIT_DETAIL_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___TRAILER_LOCATION_STATUS as TRAILER_LOCATION_STATUS", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___CONVEYABLE as CONVEYABLE", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"SQ_Shortcut_to_WM_TRAILER_REF_PRE___PRODUCT_CLASS as PRODUCT_CLASS" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 19

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_TRAILER_REF, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_TRAILER_REF_temp = SQ_Shortcut_to_WM_TRAILER_REF.toDF(*["SQ_Shortcut_to_WM_TRAILER_REF___" + col for col in SQ_Shortcut_to_WM_TRAILER_REF.columns])

JNR_WM_TRAILER_REF = SQ_Shortcut_to_WM_TRAILER_REF_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_TRAILER_REF_temp.SQ_Shortcut_to_WM_TRAILER_REF___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_TRAILER_REF_temp.SQ_Shortcut_to_WM_TRAILER_REF___WM_TRAILER_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___TRAILER_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___TRAILER_ID as TRAILER_ID", 
	"JNR_SITE_PROFILE___TRAILER_STATUS as TRAILER_STATUS", 
	"JNR_SITE_PROFILE___CURRENT_LOCATION_ID as CURRENT_LOCATION_ID", 
	"JNR_SITE_PROFILE___ASSIGNED_LOCATION_ID as ASSIGNED_LOCATION_ID", 
	"JNR_SITE_PROFILE___ACTIVE_VISIT_ID as ACTIVE_VISIT_ID", 
	"JNR_SITE_PROFILE___ACTIVE_VISIT_DETAIL_ID as ACTIVE_VISIT_DETAIL_ID", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SITE_PROFILE___TRAILER_LOCATION_STATUS as TRAILER_LOCATION_STATUS", 
	"JNR_SITE_PROFILE___CONVEYABLE as CONVEYABLE", 
	"JNR_SITE_PROFILE___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"JNR_SITE_PROFILE___PRODUCT_CLASS as PRODUCT_CLASS", 
	"SQ_Shortcut_to_WM_TRAILER_REF___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF___WM_TRAILER_ID as i_WM_TRAILER_ID", 
	"SQ_Shortcut_to_WM_TRAILER_REF___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_REF___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_REF___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_TRAILER_REF_temp = JNR_WM_TRAILER_REF.toDF(*["JNR_WM_TRAILER_REF___" + col for col in JNR_WM_TRAILER_REF.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_TRAILER_REF_temp.selectExpr( 
	"JNR_WM_TRAILER_REF___LOCATION_ID as LOCATION_ID1", 
	"JNR_WM_TRAILER_REF___TRAILER_ID as TRAILER_ID", 
	"JNR_WM_TRAILER_REF___TRAILER_STATUS as TRAILER_STATUS", 
	"JNR_WM_TRAILER_REF___CURRENT_LOCATION_ID as CURRENT_LOCATION_ID", 
	"JNR_WM_TRAILER_REF___ASSIGNED_LOCATION_ID as ASSIGNED_LOCATION_ID", 
	"JNR_WM_TRAILER_REF___ACTIVE_VISIT_ID as ACTIVE_VISIT_ID", 
	"JNR_WM_TRAILER_REF___ACTIVE_VISIT_DETAIL_ID as ACTIVE_VISIT_DETAIL_ID", 
	"JNR_WM_TRAILER_REF___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_TRAILER_REF___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_WM_TRAILER_REF___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_WM_TRAILER_REF___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_TRAILER_REF___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_TRAILER_REF___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_WM_TRAILER_REF___TRAILER_LOCATION_STATUS as TRAILER_LOCATION_STATUS", 
	"JNR_WM_TRAILER_REF___CONVEYABLE as CONVEYABLE", 
	"JNR_WM_TRAILER_REF___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"JNR_WM_TRAILER_REF___PRODUCT_CLASS as PRODUCT_CLASS", 
	"JNR_WM_TRAILER_REF___i_WM_TRAILER_ID as i_WM_TRAILER_ID", 
	"JNR_WM_TRAILER_REF___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_TRAILER_REF___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_TRAILER_REF___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_TRAILER_ID IS NULL OR (NOT i_WM_TRAILER_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___TRAILER_ID as TRAILER_ID", 
	"FIL_UNCHANGED_RECORDS___TRAILER_STATUS as TRAILER_STATUS", 
	"FIL_UNCHANGED_RECORDS___CURRENT_LOCATION_ID as CURRENT_LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___ASSIGNED_LOCATION_ID as ASSIGNED_LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___ACTIVE_VISIT_ID as ACTIVE_VISIT_ID", 
	"FIL_UNCHANGED_RECORDS___ACTIVE_VISIT_DETAIL_ID as ACTIVE_VISIT_DETAIL_ID", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___TRAILER_LOCATION_STATUS as TRAILER_LOCATION_STATUS", 
	"FIL_UNCHANGED_RECORDS___CONVEYABLE as CONVEYABLE", 
	"FIL_UNCHANGED_RECORDS___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"FIL_UNCHANGED_RECORDS___PRODUCT_CLASS as PRODUCT_CLASS", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_WM_TRAILER_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID1 as LOCATION_ID1", 
	"EXP_UPD_VALIDATOR___TRAILER_ID as TRAILER_ID", 
	"EXP_UPD_VALIDATOR___TRAILER_STATUS as TRAILER_STATUS", 
	"EXP_UPD_VALIDATOR___CURRENT_LOCATION_ID as CURRENT_LOCATION_ID", 
	"EXP_UPD_VALIDATOR___ASSIGNED_LOCATION_ID as ASSIGNED_LOCATION_ID", 
	"EXP_UPD_VALIDATOR___ACTIVE_VISIT_ID as ACTIVE_VISIT_ID", 
	"EXP_UPD_VALIDATOR___ACTIVE_VISIT_DETAIL_ID as ACTIVE_VISIT_DETAIL_ID", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_UPD_VALIDATOR___TRAILER_LOCATION_STATUS as TRAILER_LOCATION_STATUS", 
	"EXP_UPD_VALIDATOR___CONVEYABLE as CONVEYABLE", 
	"EXP_UPD_VALIDATOR___PROTECTION_LEVEL as PROTECTION_LEVEL", 
	"EXP_UPD_VALIDATOR___PRODUCT_CLASS as PRODUCT_CLASS", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_TRAILER_REF, type TARGET 
# COLUMN COUNT: 19

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_TRAILER_ID = target.WM_TRAILER_ID"""
  refined_perf_table = "WM_TRAILER_REF"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_TRAILER_REF", "WM_TRAILER_REF", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_TRAILER_REF", "WM_TRAILER_REF","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	