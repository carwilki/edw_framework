#Code converted on 2023-06-22 15:25:33
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
refined_perf_table = f"{refine}.WM_PURCHASE_ORDERS_LINE_STATUS"
raw_perf_table = f"{raw}.WM_PURCHASE_ORDERS_LINE_STATUS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS = spark.sql(f"""SELECT
LOCATION_ID,
WM_PURCHASE_ORDERS_LINE_STATUS,
WM_PURCHASE_ORDERS_LINE_STATUS_DESC,
WM_PURCHASE_ORDERS_LINE_STATUS_NOTE,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_PURCHASE_ORDERS_LINE_STATUS IN (SELECT PURCHASE_ORDERS_LINE_STATUS FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE = spark.sql(f"""SELECT
DC_NBR,
PURCHASE_ORDERS_LINE_STATUS,
DESCRIPTION,
NOTE
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE_temp = SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE___DC_NBR as int) as DC_NBR_EXP", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE___PURCHASE_ORDERS_LINE_STATUS as PURCHASE_ORDERS_LINE_STATUS", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_PRE___NOTE as NOTE" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 6

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_PURCHASE_ORDERS_LINE_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_temp = SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS.toDF(*["SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___" + col for col in SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS.columns])

JNR_WM_PURCHASE_ORDERS_LINE_STATUS = SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_temp.SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS_temp.SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PURCHASE_ORDERS_LINE_STATUS],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___PURCHASE_ORDERS_LINE_STATUS as PURCHASE_ORDERS_LINE_STATUS", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___NOTE as NOTE", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___LOCATION_ID as in_LOCATION_ID", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS as WM_PURCHASE_ORDERS_LINE_STATUS", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS_DESC as WM_PURCHASE_ORDERS_LINE_STATUS_DESC", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS_NOTE as WM_PURCHASE_ORDERS_LINE_STATUS_NOTE", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_PURCHASE_ORDERS_LINE_STATUS_temp = JNR_WM_PURCHASE_ORDERS_LINE_STATUS.toDF(*["JNR_WM_PURCHASE_ORDERS_LINE_STATUS___" + col for col in JNR_WM_PURCHASE_ORDERS_LINE_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_PURCHASE_ORDERS_LINE_STATUS_temp.selectExpr( 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___PURCHASE_ORDERS_LINE_STATUS as PURCHASE_ORDERS_LINE_STATUS", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___DESCRIPTION as DESCRIPTION", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___NOTE as NOTE", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___in_LOCATION_ID as in_LOCATION_ID", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS as WM_PURCHASE_ORDERS_LINE_STATUS", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS_DESC as WM_PURCHASE_ORDERS_LINE_STATUS_DESC", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___WM_PURCHASE_ORDERS_LINE_STATUS_NOTE as WM_PURCHASE_ORDERS_LINE_STATUS_NOTE", 
	"JNR_WM_PURCHASE_ORDERS_LINE_STATUS___in_LOAD_TSTMP as in_LOAD_TSTMP").filter(expr("WM_PURCHASE_ORDERS_LINE_STATUS IS NULL OR (WM_PURCHASE_ORDERS_LINE_STATUS IS NOT NULL AND (COALESCE(TRIM(WM_PURCHASE_ORDERS_LINE_STATUS_DESC), '') <> COALESCE(TRIM(DESCRIPTION), '') OR COALESCE(TRIM(WM_PURCHASE_ORDERS_LINE_STATUS_NOTE), '') <> COALESCE(TRIM(NOTE), '')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___PURCHASE_ORDERS_LINE_STATUS as PURCHASE_ORDERS_LINE_STATUS", 
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_RECORDS___NOTE as NOTE", 
	"FIL_UNCHANGED_RECORDS___in_LOCATION_ID as in_LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___WM_PURCHASE_ORDERS_LINE_STATUS as WM_PURCHASE_ORDERS_LINE_STATUS", 
	"FIL_UNCHANGED_RECORDS___WM_PURCHASE_ORDERS_LINE_STATUS_DESC as WM_PURCHASE_ORDERS_LINE_STATUS_DESC", 
	"FIL_UNCHANGED_RECORDS___WM_PURCHASE_ORDERS_LINE_STATUS_NOTE as WM_PURCHASE_ORDERS_LINE_STATUS_NOTE", 
	"FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP as in_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___WM_PURCHASE_ORDERS_LINE_STATUS IS NULL, 1, 2) as o_UPD_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPD_VALIDATOR___PURCHASE_ORDERS_LINE_STATUS as PURCHASE_ORDERS_LINE_STATUS", 
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_UPD_VALIDATOR___NOTE as NOTE", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(1))lit(0)).when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR == (lit(2))lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_PURCHASE_ORDERS_LINE_STATUS1, type TARGET 
# COLUMN COUNT: 6

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_PURCHASE_ORDERS_LINE_STATUS = target.WM_PURCHASE_ORDERS_LINE_STATUS"""
  # refined_perf_table = "WM_PURCHASE_ORDERS_LINE_STATUS"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_PURCHASE_ORDERS_LINE_STATUS", "WM_PURCHASE_ORDERS_LINE_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_PURCHASE_ORDERS_LINE_STATUS", "WM_PURCHASE_ORDERS_LINE_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	