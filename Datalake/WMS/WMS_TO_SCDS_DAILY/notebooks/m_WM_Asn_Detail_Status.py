#Code converted on 2023-06-15 14:12:01
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

# parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_ASN_DETAIL_STATUS"
raw_perf_table = f"{raw}.WM_ASN_DETAIL_STATUS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"

# Read in relation source variables

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 3


SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE = (spark.sql( f"""SELECT
DC_NBR,
ASN_DETAIL_STATUS,
DESCRIPTION
FROM {raw_perf_table}""")
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE_temp = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_PRE___DESCRIPTION as DESCRIPTION" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ASN_DETAIL_STATUS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_ASN_DETAIL_STATUS = (spark.sql( f"""SELECT
LOCATION_ID,
WM_ASN_DETAIL_STATUS,
WM_ASN_DETAIL_STATUS_DESC,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_ASN_DETAIL_STATUS IN (SELECT ASN_DETAIL_STATUS FROM {raw_perf_table})""")
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONV_temp = EXP_INT_CONV.toDF(*["EXP_INT_CONV___" + col for col in EXP_INT_CONV.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = EXP_INT_CONV_temp.join(SQ_Shortcut_to_SITE_PROFILE_temp,[EXP_INT_CONV_temp.EXP_INT_CONV___o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR],'inner').selectExpr( \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR", \
	"EXP_INT_CONV___o_DC_NBR as DC_NBR", \
	"EXP_INT_CONV___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"EXP_INT_CONV___DESCRIPTION as DESCRIPTION")

# COMMAND ----------
# Processing node JNR_WM_ASN_DETAIL_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS.toDF(*["SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___" + col for col in SQ_Shortcut_to_WM_ASN_DETAIL_STATUS.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_ASN_DETAIL_STATUS = SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp.SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_ASN_DETAIL_STATUS_temp.SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___WM_ASN_DETAIL_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ASN_DETAIL_STATUS],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___WM_ASN_DETAIL_STATUS as i_WM_ASN_DETAIL_STATUS", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___WM_ASN_DETAIL_STATUS_DESC as i_WM_ASN_DETAIL_STATUS_DESC", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___UPDATE_TSTMP as i_UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_ASN_DETAIL_STATUS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ASN_DETAIL_STATUS_temp = JNR_WM_ASN_DETAIL_STATUS.toDF(*["JNR_WM_ASN_DETAIL_STATUS___" + col for col in JNR_WM_ASN_DETAIL_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_ASN_DETAIL_STATUS_temp.selectExpr( \
	"JNR_WM_ASN_DETAIL_STATUS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ASN_DETAIL_STATUS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"JNR_WM_ASN_DETAIL_STATUS___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_ASN_DETAIL_STATUS___i_LOCATION_ID as i_LOCATION_ID", \
	"JNR_WM_ASN_DETAIL_STATUS___i_WM_ASN_DETAIL_STATUS as i_WM_ASN_DETAIL_STATUS", \
	"JNR_WM_ASN_DETAIL_STATUS___i_WM_ASN_DETAIL_STATUS_DESC as i_WM_ASN_DETAIL_STATUS_DESC", \
	"JNR_WM_ASN_DETAIL_STATUS___i_UPDATE_TSTMP as i_UPDATE_TSTMP", \
	"JNR_WM_ASN_DETAIL_STATUS___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("i_WM_ASN_DETAIL_STATUS is Null OR ( i_WM_ASN_DETAIL_STATUS is Not Null AND  COALESCE(DESCRIPTION, '') != COALESCE(i_WM_ASN_DETAIL_STATUS_DESC, ''))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_WM_ASN_DETAIL_STATUS IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( \
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_OUTPUT_VALIDATOR___ASN_DETAIL_STATUS as ASN_DETAIL_STATUS", \
	"EXP_OUTPUT_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ASN_DETAIL_STATUS1, type TARGET 
# COLUMN COUNT: 5

Shortcut_to_WM_ASN_DETAIL_STATUS1 = UPD_INS_UPD.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(ASN_DETAIL_STATUS AS BIGINT) as WM_ASN_DETAIL_STATUS", 
	"CAST(DESCRIPTION AS STRING) as WM_ASN_DETAIL_STATUS_DESC", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ASN_DETAIL_STATUS = target.WM_ASN_DETAIL_STATUS"""
  executeMerge(Shortcut_to_WM_ASN_DETAIL_STATUS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ASN_DETAIL_STATUS", "WM_ASN_DETAIL_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ASN_DETAIL_STATUS", "WM_ASN_DETAIL_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	