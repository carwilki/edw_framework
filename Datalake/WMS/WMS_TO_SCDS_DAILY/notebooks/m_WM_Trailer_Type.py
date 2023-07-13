#Code converted on 2023-06-22 21:02:51
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
refined_perf_table = f"{refine}.WM_TRAILER_TYPE"
raw_perf_table = f"{raw}.WM_TRAILER_TYPE_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_TYPE_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_TRAILER_TYPE_PRE = spark.sql(f"""SELECT
DC_NBR,
TRAILER_TYPE,
DESCRIPTION,
CREATED_DTTM,
LAST_UPDATED_DTTM
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_TYPE, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_WM_TRAILER_TYPE = spark.sql(f"""SELECT
LOCATION_ID,
WM_TRAILER_TYPE_ID,
WM_TRAILER_TYPE_DESC,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_TRAILER_TYPE_ID IN ( SELECT TRAILER_TYPE FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_TRAILER_TYPE_PRE_temp = SQ_Shortcut_to_WM_TRAILER_TYPE_PRE.toDF(*["SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___" + col for col in SQ_Shortcut_to_WM_TRAILER_TYPE_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_TRAILER_TYPE_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___TRAILER_TYPE as TRAILER_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 7

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_TRAILER_TYPE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_TRAILER_TYPE_temp = SQ_Shortcut_to_WM_TRAILER_TYPE.toDF(*["SQ_Shortcut_to_WM_TRAILER_TYPE___" + col for col in SQ_Shortcut_to_WM_TRAILER_TYPE.columns])

JNR_TRAILER_TYPE = SQ_Shortcut_to_WM_TRAILER_TYPE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_TRAILER_TYPE_temp.SQ_Shortcut_to_WM_TRAILER_TYPE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_TRAILER_TYPE_temp.SQ_Shortcut_to_WM_TRAILER_TYPE___WM_TRAILER_TYPE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___TRAILER_TYPE],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___TRAILER_TYPE as TRAILER_TYPE", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___LOCATION_ID as in_LOCATION_ID", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___WM_TRAILER_TYPE_ID as WM_TRAILER_TYPE_ID", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___WM_TRAILER_TYPE_DESC as WM_TRAILER_TYPE_DESC", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___UPDATE_TSTMP as UPDATE_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_TYPE___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAILER_TYPE_temp = JNR_TRAILER_TYPE.toDF(*["JNR_TRAILER_TYPE___" + col for col in JNR_TRAILER_TYPE.columns])

FIL_UNCHANGED_REC = JNR_TRAILER_TYPE_temp.selectExpr( 
	"JNR_TRAILER_TYPE___TRAILER_TYPE as TRAILER_TYPE", 
	"JNR_TRAILER_TYPE___DESCRIPTION as DESCRIPTION", 
	"JNR_TRAILER_TYPE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_TRAILER_TYPE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_TRAILER_TYPE___LOCATION_ID as LOCATION_ID", 
	"JNR_TRAILER_TYPE___in_LOCATION_ID as in_LOCATION_ID", 
	"JNR_TRAILER_TYPE___WM_TRAILER_TYPE_ID as WM_TRAILER_TYPE_ID", 
	"JNR_TRAILER_TYPE___WM_TRAILER_TYPE_DESC as WM_TRAILER_TYPE_DESC", 
	"JNR_TRAILER_TYPE___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"JNR_TRAILER_TYPE___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"JNR_TRAILER_TYPE___UPDATE_TSTMP as UPDATE_TSTMP", 
	"JNR_TRAILER_TYPE___in_LOAD_TSTMP as in_LOAD_TSTMP").filter(expr("WM_TRAILER_TYPE_ID IS NULL OR (NOT WM_TRAILER_TYPE_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( 
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_REC___TRAILER_TYPE as TRAILER_TYPE", 
	"FIL_UNCHANGED_REC___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_REC___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_REC___WM_TRAILER_TYPE_ID as WM_TRAILER_TYPE_ID", 
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP(), FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP_exp", 
	"IF(FIL_UNCHANGED_REC___WM_TRAILER_TYPE_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR", 
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPD_VALIDATOR___TRAILER_TYPE as TRAILER_TYPE", 
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP_exp as LOAD_TSTMP_exp", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)),lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_TRAILER_TYPE1, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_WM_TRAILER_TYPE1 = UPD_INS_UPD.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(TRAILER_TYPE AS BIGINT) as WM_TRAILER_TYPE_ID", 
	"CAST(DESCRIPTION AS STRING) as WM_TRAILER_TYPE_DESC", 
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP", 
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP_exp AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_TRAILER_TYPE_ID = target.WM_TRAILER_TYPE_ID"""
  # refined_perf_table = "WM_TRAILER_TYPE"
  executeMerge(Shortcut_to_WM_TRAILER_TYPE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_TRAILER_TYPE", "WM_TRAILER_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_TRAILER_TYPE", "WM_TRAILER_TYPE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
