#Code converted on 2023-06-27 09:40:29
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
refined_perf_table = f"{refine}.WM_LPN_FACILITY_STATUS"
raw_perf_table = f"{raw}.WM_LPN_FACILITY_STATUS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE = spark.sql(f"""SELECT
DC_NBR,
LPN_FACILITY_STATUS,
INBOUND_OUTBOUND_INDICATOR,
DESCRIPTION,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE_temp = SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE.columns])

EXP_TRANS = SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___LPN_FACILITY_STATUS as LPN_FACILITY_STATUS", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_FACILITY_STATUS, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_LPN_FACILITY_STATUS = spark.sql(f"""SELECT
LOCATION_ID,
WM_LPN_FACILITY_STATUS,
INBOUND_OUTBOUND_IND,
WM_LPN_FACILITY_STATUS_DESC,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_LPN_FACILITY_STATUS IN (SELECT LPN_FACILITY_STATUS FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 7

JNR_SITE_PROFILE = EXP_TRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_TRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LPN_FACILITY_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_temp = SQ_Shortcut_to_WM_LPN_FACILITY_STATUS.toDF(*["SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___" + col for col in SQ_Shortcut_to_WM_LPN_FACILITY_STATUS.columns])

JNR_WM_LPN_FACILITY_STATUS = SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_temp.SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_temp.SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___WM_LPN_FACILITY_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LPN_FACILITY_STATUS, SQ_Shortcut_to_WM_LPN_FACILITY_STATUS_temp.SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___INBOUND_OUTBOUND_IND == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___INBOUND_OUTBOUND_INDICATOR],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___STORE_NBR as STORE_NBR", \
	"JNR_SITE_PROFILE___LPN_FACILITY_STATUS as LPN_FACILITY_STATUS", \
	"JNR_SITE_PROFILE___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___WM_LPN_FACILITY_STATUS as WM_LPN_FACILITY_STATUS", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___INBOUND_OUTBOUND_IND as INBOUND_OUTBOUND_IND", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___WM_LPN_FACILITY_STATUS_DESC as WM_LPN_FACILITY_STATUS_DESC", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_FACILITY_STATUS___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LPN_FACILITY_STATUS_temp = JNR_WM_LPN_FACILITY_STATUS.toDF(*["JNR_WM_LPN_FACILITY_STATUS___" + col for col in JNR_WM_LPN_FACILITY_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_LPN_FACILITY_STATUS_temp.selectExpr( \
	"JNR_WM_LPN_FACILITY_STATUS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LPN_FACILITY_STATUS___LPN_FACILITY_STATUS as LPN_FACILITY_STATUS", \
	"JNR_WM_LPN_FACILITY_STATUS___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"JNR_WM_LPN_FACILITY_STATUS___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_LPN_FACILITY_STATUS___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_WM_LPN_FACILITY_STATUS___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_LPN_FACILITY_STATUS___WM_LPN_FACILITY_STATUS as WM_LPN_FACILITY_STATUS", \
	"JNR_WM_LPN_FACILITY_STATUS___INBOUND_OUTBOUND_IND as INBOUND_OUTBOUND_IND", \
	"JNR_WM_LPN_FACILITY_STATUS___WM_LPN_FACILITY_STATUS_DESC as WM_LPN_FACILITY_STATUS_DESC", \
	"JNR_WM_LPN_FACILITY_STATUS___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_LPN_FACILITY_STATUS is Null OR (  WM_LPN_FACILITY_STATUS is NOT Null AND COALESCE(ltrim ( rtrim ( DESCRIPTION )), '') != COALESCE(ltrim ( rtrim ( WM_LPN_FACILITY_STATUS )), '') )").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___LPN_FACILITY_STATUS as LPN_FACILITY_STATUS", \
	"FIL_UNCHANGED_RECORDS___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", \
	"FIL_UNCHANGED_RECORDS___LOAD_TSTMP as LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___WM_LPN_FACILITY_STATUS as WM_LPN_FACILITY_STATUS", \
	"FIL_UNCHANGED_RECORDS___INBOUND_OUTBOUND_IND as INBOUND_OUTBOUND_IND", \
	"FIL_UNCHANGED_RECORDS___WM_LPN_FACILITY_STATUS_DESC as WM_LPN_FACILITY_STATUS_DESC", \
	"FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP) as LOAD_TSTMP_EXP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___WM_LPN_FACILITY_STATUS IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPDATE = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___LPN_FACILITY_STATUS as LPN_FACILITY_STATUS", \
	"EXP_UPD_VALIDATOR___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP_EXP as LOAD_TSTMP_EXP", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(1)), lit(0)).when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LPN_FACILITY_STATUS1, type TARGET 
# COLUMN COUNT: 6

Shortcut_to_WM_LPN_FACILITY_STATUS1 = UPD_INS_UPDATE.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(LPN_FACILITY_STATUS AS BIGINT) as WM_LPN_FACILITY_STATUS", 
	"CAST(INBOUND_OUTBOUND_INDICATOR AS STRING) as INBOUND_OUTBOUND_IND", 
	"CAST(DESCRIPTION AS STRING) as WM_LPN_FACILITY_STATUS_DESC", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LPN_FACILITY_STATUS = target.WM_LPN_FACILITY_STATUS AND source.INBOUND_OUTBOUND_IND = target.INBOUND_OUTBOUND_IND"""
#   refined_perf_table = "WM_LPN_FACILITY_STATUS"
  executeMerge(Shortcut_to_WM_LPN_FACILITY_STATUS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LPN_FACILITY_STATUS", "WM_LPN_FACILITY_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LPN_FACILITY_STATUS", "WM_LPN_FACILITY_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	