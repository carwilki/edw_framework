#Code converted on 2023-06-24 13:33:33
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
refined_perf_table = f"{refine}.WM_STANDARD_UOM"
raw_perf_table = f"{raw}.WM_STANDARD_UOM_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_STANDARD_UOM_PRE, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_WM_STANDARD_UOM_PRE = spark.sql(f"""SELECT
DC_NBR,
STANDARD_UOM,
STANDARD_UOM_TYPE,
ABBREVIATION,
DESCRIPTION,
UOM_SYSTEM,
IS_TYPE_SYS_DFLT,
UNITS_IN_TYPE_SYS_DFLT,
IS_DB_UOM,
IS_SYSTEM_DEFINED,
CREATED_DTTM,
LAST_UPDATED_DTTM
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_STANDARD_UOM, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_STANDARD_UOM = spark.sql(f"""SELECT
LOCATION_ID,
WM_STANDARD_UOM_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_STANDARD_UOM_ID IN (SELECT STANDARD_UOM FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_STANDARD_UOM_PRE_temp = SQ_Shortcut_to_WM_STANDARD_UOM_PRE.toDF(*["SQ_Shortcut_to_WM_STANDARD_UOM_PRE___" + col for col in SQ_Shortcut_to_WM_STANDARD_UOM_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_STANDARD_UOM_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_STANDARD_UOM_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___STANDARD_UOM as STANDARD_UOM", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___STANDARD_UOM_TYPE as STANDARD_UOM_TYPE", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___ABBREVIATION as ABBREVIATION", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___UOM_SYSTEM as UOM_SYSTEM", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___IS_TYPE_SYS_DFLT as IS_TYPE_SYS_DFLT", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___UNITS_IN_TYPE_SYS_DFLT as UNITS_IN_TYPE_SYS_DFLT", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___IS_DB_UOM as IS_DB_UOM", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___IS_SYSTEM_DEFINED as IS_SYSTEM_DEFINED", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_STANDARD_UOM_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 14

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_STANDARD_UOM, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_STANDARD_UOM_temp = SQ_Shortcut_to_WM_STANDARD_UOM.toDF(*["SQ_Shortcut_to_WM_STANDARD_UOM___" + col for col in SQ_Shortcut_to_WM_STANDARD_UOM.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_STANDARD_UOM = SQ_Shortcut_to_WM_STANDARD_UOM_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_STANDARD_UOM_temp.SQ_Shortcut_to_WM_STANDARD_UOM___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_STANDARD_UOM_temp.SQ_Shortcut_to_WM_STANDARD_UOM___WM_STANDARD_UOM_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___STANDARD_UOM],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___STANDARD_UOM as STANDARD_UOM", 
	"JNR_SITE_PROFILE___STANDARD_UOM_TYPE as STANDARD_UOM_TYPE", 
	"JNR_SITE_PROFILE___ABBREVIATION as ABBREVIATION", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___UOM_SYSTEM as UOM_SYSTEM", 
	"JNR_SITE_PROFILE___IS_TYPE_SYS_DFLT as IS_TYPE_SYS_DFLT", 
	"JNR_SITE_PROFILE___UNITS_IN_TYPE_SYS_DFLT as UNITS_IN_TYPE_SYS_DFLT", 
	"JNR_SITE_PROFILE___IS_DB_UOM as IS_DB_UOM", 
	"JNR_SITE_PROFILE___IS_SYSTEM_DEFINED as IS_SYSTEM_DEFINED", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_STANDARD_UOM___LOCATION_ID as i_LOCATION_ID1", 
	"SQ_Shortcut_to_WM_STANDARD_UOM___WM_STANDARD_UOM_ID as i_WM_STANDARD_UOM_ID", 
	"SQ_Shortcut_to_WM_STANDARD_UOM___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_STANDARD_UOM___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_STANDARD_UOM___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_STANDARD_UOM_temp = JNR_WM_STANDARD_UOM.toDF(*["JNR_WM_STANDARD_UOM___" + col for col in JNR_WM_STANDARD_UOM.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_STANDARD_UOM_temp.selectExpr( 
	"JNR_WM_STANDARD_UOM___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_STANDARD_UOM___STANDARD_UOM as STANDARD_UOM", 
	"JNR_WM_STANDARD_UOM___STANDARD_UOM_TYPE as STANDARD_UOM_TYPE", 
	"JNR_WM_STANDARD_UOM___ABBREVIATION as ABBREVIATION", 
	"JNR_WM_STANDARD_UOM___DESCRIPTION as DESCRIPTION", 
	"JNR_WM_STANDARD_UOM___UOM_SYSTEM as UOM_SYSTEM", 
	"JNR_WM_STANDARD_UOM___IS_TYPE_SYS_DFLT as IS_TYPE_SYS_DFLT", 
	"JNR_WM_STANDARD_UOM___UNITS_IN_TYPE_SYS_DFLT as UNITS_IN_TYPE_SYS_DFLT", 
	"JNR_WM_STANDARD_UOM___IS_DB_UOM as IS_DB_UOM", 
	"JNR_WM_STANDARD_UOM___IS_SYSTEM_DEFINED as IS_SYSTEM_DEFINED", 
	"JNR_WM_STANDARD_UOM___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_STANDARD_UOM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_STANDARD_UOM___i_WM_STANDARD_UOM_ID as i_WM_STANDARD_UOM_ID", 
	"JNR_WM_STANDARD_UOM___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_STANDARD_UOM___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_STANDARD_UOM___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_STANDARD_UOM_ID IS NULL OR (NOT i_WM_STANDARD_UOM_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___STANDARD_UOM as STANDARD_UOM", 
	"FIL_UNCHANGED_RECORDS___STANDARD_UOM_TYPE as STANDARD_UOM_TYPE", 
	"FIL_UNCHANGED_RECORDS___ABBREVIATION as ABBREVIATION", 
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_RECORDS___UOM_SYSTEM as UOM_SYSTEM", 
	"FIL_UNCHANGED_RECORDS___IS_TYPE_SYS_DFLT as IS_TYPE_SYS_DFLT", 
	"FIL_UNCHANGED_RECORDS___UNITS_IN_TYPE_SYS_DFLT as UNITS_IN_TYPE_SYS_DFLT", 
	"FIL_UNCHANGED_RECORDS___IS_DB_UOM as IS_DB_UOM", 
	"FIL_UNCHANGED_RECORDS___IS_SYSTEM_DEFINED as IS_SYSTEM_DEFINED", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_STANDARD_UOM_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( 
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_OUTPUT_VALIDATOR___STANDARD_UOM as STANDARD_UOM", 
	"EXP_OUTPUT_VALIDATOR___STANDARD_UOM_TYPE as STANDARD_UOM_TYPE", 
	"EXP_OUTPUT_VALIDATOR___ABBREVIATION as ABBREVIATION", 
	"EXP_OUTPUT_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_OUTPUT_VALIDATOR___UOM_SYSTEM as UOM_SYSTEM", 
	"EXP_OUTPUT_VALIDATOR___IS_TYPE_SYS_DFLT as IS_TYPE_SYS_DFLT", 
	"EXP_OUTPUT_VALIDATOR___UNITS_IN_TYPE_SYS_DFLT as UNITS_IN_TYPE_SYS_DFLT", 
	"EXP_OUTPUT_VALIDATOR___IS_DB_UOM as IS_DB_UOM", 
	"EXP_OUTPUT_VALIDATOR___IS_SYSTEM_DEFINED as IS_SYSTEM_DEFINED", 
	"EXP_OUTPUT_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_OUTPUT_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1))lit(0)).when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2))lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_STANDARD_UOM1, type TARGET 
# COLUMN COUNT: 14

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_STANDARD_UOM_ID = target.WM_STANDARD_UOM_ID"""
  # refined_perf_table = "WM_STANDARD_UOM"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_STANDARD_UOM", "WM_STANDARD_UOM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_STANDARD_UOM", "WM_STANDARD_UOM","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	