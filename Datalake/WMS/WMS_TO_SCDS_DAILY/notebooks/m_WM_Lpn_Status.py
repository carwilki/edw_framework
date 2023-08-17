#Code converted on 2023-06-20 18:04:13
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
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_LPN_STATUS"
raw_perf_table = f"{raw}.WM_LPN_STATUS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_STATUS, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_WM_LPN_STATUS = spark.sql(f"""SELECT
LOCATION_ID,
WM_LPN_STATUS,
WM_LPN_STATUS_DESC,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_LPN_STATUS IN
(SELECT LPN_STATUS FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_WM_LPN_STATUS_PRE = spark.sql(f"""SELECT
DC_NBR,
LPN_STATUS,
DESCRIPTION
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LPN_STATUS_PRE_temp = SQ_Shortcut_to_WM_LPN_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_LPN_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_LPN_STATUS_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_LPN_STATUS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LPN_STATUS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LPN_STATUS_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_LPN_STATUS_PRE___LPN_STATUS as LPN_STATUS", \
	"SQ_Shortcut_to_WM_LPN_STATUS_PRE___DESCRIPTION as DESCRIPTION" \
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 5

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LPN_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_LPN_STATUS_temp = SQ_Shortcut_to_WM_LPN_STATUS.toDF(*["SQ_Shortcut_to_WM_LPN_STATUS___" + col for col in SQ_Shortcut_to_WM_LPN_STATUS.columns])

JNR_WM_LPN_STATUS = SQ_Shortcut_to_WM_LPN_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LPN_STATUS_temp.SQ_Shortcut_to_WM_LPN_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LPN_STATUS_temp.SQ_Shortcut_to_WM_LPN_STATUS___WM_LPN_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LPN_STATUS],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___LPN_STATUS as LPN_STATUS", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_LPN_STATUS___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LPN_STATUS___WM_LPN_STATUS as WM_LPN_STATUS", \
	"SQ_Shortcut_to_WM_LPN_STATUS___WM_LPN_STATUS_DESC as WM_LPN_STATUS_DESC", \
	"SQ_Shortcut_to_WM_LPN_STATUS___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_CHANGED_REC, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LPN_STATUS_temp = JNR_WM_LPN_STATUS.toDF(*["JNR_WM_LPN_STATUS___" + col for col in JNR_WM_LPN_STATUS.columns])

FIL_CHANGED_REC = JNR_WM_LPN_STATUS_temp.selectExpr( \
	"JNR_WM_LPN_STATUS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LPN_STATUS___LPN_STATUS as LPN_STATUS", \
	"JNR_WM_LPN_STATUS___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_LPN_STATUS___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_LPN_STATUS___WM_LPN_STATUS as WM_LPN_STATUS", \
	"JNR_WM_LPN_STATUS___WM_LPN_STATUS_DESC as WM_LPN_STATUS_DESC", \
	"JNR_WM_LPN_STATUS___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_LPN_STATUS is Null OR ( WM_LPN_STATUS is not Null AND \
     ( COALESCE(ltrim(rtrim(WM_LPN_STATUS_DESC)), '') != COALESCE(ltrim(rtrim(DESCRIPTION)), '')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
FIL_CHANGED_REC_temp = FIL_CHANGED_REC.toDF(*["FIL_CHANGED_REC___" + col for col in FIL_CHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_CHANGED_REC_temp.selectExpr( \
	"FIL_CHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_CHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_CHANGED_REC___LPN_STATUS as LPN_STATUS", \
	"FIL_CHANGED_REC___DESCRIPTION as DESCRIPTION", \
	"FIL_CHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_CHANGED_REC___WM_LPN_STATUS as WM_LPN_STATUS", \
	"FIL_CHANGED_REC___WM_LPN_STATUS_DESC as WM_LPN_STATUS_DESC", \
	"FIL_CHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_CHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_CHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_CHANGED_REC___WM_LPN_STATUS IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___LPN_STATUS as LPN_STATUS", \
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPD_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LPN_STATUS1, type TARGET 
# COLUMN COUNT: 5

Shortcut_to_WM_LPN_STATUS1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(LPN_STATUS AS SMALLINT) as WM_LPN_STATUS",
	"CAST(DESCRIPTION AS STRING) as WM_LPN_STATUS_DESC",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LPN_STATUS = target.WM_LPN_STATUS"""
#   refined_perf_table = "WM_LPN_STATUS"
  executeMerge(Shortcut_to_WM_LPN_STATUS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LPN_STATUS", "WM_LPN_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LPN_STATUS", "WM_LPN_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e

	