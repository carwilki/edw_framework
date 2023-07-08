#Code converted on 2023-06-26 10:03:37
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
refined_perf_table = f"{refine}.WM_DO_STATUS"
raw_perf_table = f"{raw}.WM_DO_STATUS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_DO_STATUS, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_WM_DO_STATUS = spark.sql(f"""SELECT
LOCATION_ID,
WM_ORDER_STATUS_ID,
WM_ORDER_STATUS_DESC,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_ORDER_STATUS_ID IN ( SELECT ORDER_STATUS FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_DO_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_WM_DO_STATUS_PRE = spark.sql(f"""SELECT
DC_NBR,
ORDER_STATUS,
DESCRIPTION,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_DO_STATUS_PRE_temp = SQ_Shortcut_to_WM_DO_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_DO_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_DO_STATUS_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_DO_STATUS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_DO_STATUS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_DO_STATUS_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_DO_STATUS_PRE___DC_NBR as DC_NBR", \
	"SQ_Shortcut_to_WM_DO_STATUS_PRE___ORDER_STATUS as ORDER_STATUS", \
	"SQ_Shortcut_to_WM_DO_STATUS_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_DO_STATUS_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONVERSION_temp = EXP_INT_CONVERSION.toDF(*["EXP_INT_CONVERSION___" + col for col in EXP_INT_CONVERSION.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXP_INT_CONVERSION_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_INT_CONVERSION_temp.EXP_INT_CONVERSION___o_DC_NBR],'inner').selectExpr( \
	"EXP_INT_CONVERSION___o_DC_NBR as o_DC_NBR", \
	"EXP_INT_CONVERSION___DC_NBR as DC_NBR", \
	"EXP_INT_CONVERSION___ORDER_STATUS as ORDER_STATUS", \
	"EXP_INT_CONVERSION___DESCRIPTION as DESCRIPTION", \
	"EXP_INT_CONVERSION___LOAD_TSTMP as in_LOAD_TSTMP", \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR")

# COMMAND ----------
# Processing node JNR_Do_Status, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_DO_STATUS_temp = SQ_Shortcut_to_WM_DO_STATUS.toDF(*["SQ_Shortcut_to_WM_DO_STATUS___" + col for col in SQ_Shortcut_to_WM_DO_STATUS.columns])

JNR_Do_Status = SQ_Shortcut_to_WM_DO_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_DO_STATUS_temp.SQ_Shortcut_to_WM_DO_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_DO_STATUS_temp.SQ_Shortcut_to_WM_DO_STATUS___WM_ORDER_STATUS_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ORDER_STATUS],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___ORDER_STATUS as ORDER_STATUS", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_DO_STATUS___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_DO_STATUS___WM_ORDER_STATUS_ID as WM_ORDER_STATUS_ID", \
	"SQ_Shortcut_to_WM_DO_STATUS___WM_ORDER_STATUS_DESC as WM_ORDER_STATUS_DESC", \
	"SQ_Shortcut_to_WM_DO_STATUS___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_REC_UNCHANGED, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_Do_Status_temp = JNR_Do_Status.toDF(*["JNR_Do_Status___" + col for col in JNR_Do_Status.columns])

FIL_REC_UNCHANGED = JNR_Do_Status_temp.selectExpr( \
	"JNR_Do_Status___ORDER_STATUS as ORDER_STATUS", \
	"JNR_Do_Status___DESCRIPTION as DESCRIPTION", \
	"JNR_Do_Status___LOCATION_ID as LOCATION_ID", \
	"JNR_Do_Status___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_Do_Status___WM_ORDER_STATUS_ID as WM_ORDER_STATUS_ID", \
	"JNR_Do_Status___WM_ORDER_STATUS_DESC as WM_ORDER_STATUS_DESC", \
	"JNR_Do_Status___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_ORDER_STATUS_ID is Null OR (  WM_ORDER_STATUS_ID is NOT Null AND ( COALESCE(ltrim ( rtrim ( upper ( DESCRIPTION ) ) ), '') != COALESCE(ltrim ( rtrim ( upper ( WM_ORDER_STATUS_DESC ) ) ), '')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
FIL_REC_UNCHANGED_temp = FIL_REC_UNCHANGED.toDF(*["FIL_REC_UNCHANGED___" + col for col in FIL_REC_UNCHANGED.columns])

EXP_UPD_VALIDATOR = FIL_REC_UNCHANGED_temp.selectExpr( \
	"FIL_REC_UNCHANGED___sys_row_id as sys_row_id", \
	"FIL_REC_UNCHANGED___ORDER_STATUS as ORDER_STATUS", \
	"FIL_REC_UNCHANGED___DESCRIPTION as DESCRIPTION", \
	"FIL_REC_UNCHANGED___LOCATION_ID as LOCATION_ID", \
	"FIL_REC_UNCHANGED___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_REC_UNCHANGED___WM_ORDER_STATUS_ID as WM_ORDER_STATUS_ID", \
	"FIL_REC_UNCHANGED___WM_ORDER_STATUS_DESC as WM_ORDER_STATUS_DESC", \
	"FIL_REC_UNCHANGED___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"IF(FIL_REC_UNCHANGED___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP(), FIL_REC_UNCHANGED___in_LOAD_TSTMP) as LOAD_TSTMP_exp", \
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP", \
	"IF(FIL_REC_UNCHANGED___WM_ORDER_STATUS_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___ORDER_STATUS as ORDER_STATUS", \
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP_exp as LOAD_TSTMP_exp", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(1)), lit(0)).when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_DO_STATUS1, type TARGET 
# COLUMN COUNT: 5

Shortcut_to_WM_DO_STATUS1 = UPD_INS_UPD.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(ORDER_STATUS AS BIGINT) as WM_ORDER_STATUS_ID", 
	"CAST(DESCRIPTION AS STRING) as WM_ORDER_STATUS_DESC", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP_exp AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ORDER_STATUS_ID = target.WM_ORDER_STATUS_ID"""
#   refined_perf_table = "WM_DO_STATUS"
  executeMerge(Shortcut_to_WM_DO_STATUS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_DO_STATUS", "WM_DO_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_DO_STATUS", "WM_DO_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	