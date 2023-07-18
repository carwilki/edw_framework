#Code converted on 2023-06-22 17:50:15
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
refined_perf_table = f"{refine}.WM_PURCHASE_ORDERS_STATUS"
raw_perf_table = f"{raw}.WM_PURCHASE_ORDERS_STATUS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS = spark.sql(f"""SELECT
LOCATION_ID,
WM_PURCHASE_ORDERS_STATUS,
WM_PURCHASE_ORDERS_STATUS_DESC,
WM_PURCHASE_ORDERS_STATUS_NOTE,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_PURCHASE_ORDERS_STATUS IN (SELECT PURCHASE_ORDERS_STATUS FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE = spark.sql(f"""SELECT
DC_NBR,
PURCHASE_ORDERS_STATUS,
DESCRIPTION,
NOTE
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE_temp = SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___sys_row_id as sys_row_id", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___DC_NBR as DC_NBR", 
	"cast(SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___PURCHASE_ORDERS_STATUS as PURCHASE_ORDERS_STATUS", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_PRE___NOTE as NOTE" 
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXPTRANS_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXPTRANS_temp.EXPTRANS___o_DC_NBR],'inner').selectExpr( 
	"EXPTRANS___o_DC_NBR as o_DC_NBR", 
	"EXPTRANS___PURCHASE_ORDERS_STATUS as PURCHASE_ORDERS_STATUS", 
	"EXPTRANS___DESCRIPTION as DESCRIPTION", 
	"EXPTRANS___NOTE as NOTE", 
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR") \
	.withColumn('WM_PURCHASE_ORDERS_STATUS', lit(None)) \
	.withColumn('WM_PURCHASE_ORDERS_STATUS_DESC', lit(None)) \
	.withColumn('WM_PURCHASE_ORDERS_STATUS_NOTE', lit(None)) \
	.withColumn('LOAD_TSTMP', lit(None)) \
	

# COMMAND ----------
# Processing node JNR_WM_PURCHASE_ORDERS_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_temp = SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS.toDF(*["SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___" + col for col in SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS.columns])

JNR_WM_PURCHASE_ORDERS_STATUS = SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_temp.SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS_temp.SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___WM_PURCHASE_ORDERS_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PURCHASE_ORDERS_STATUS],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___PURCHASE_ORDERS_STATUS as PURCHASE_ORDERS_STATUS", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___NOTE as NOTE", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___WM_PURCHASE_ORDERS_STATUS as i_WM_PURCHASE_ORDERS_STATUS", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___WM_PURCHASE_ORDERS_STATUS_DESC as i_WM_PURCHASE_ORDERS_STATUS_DESC", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___WM_PURCHASE_ORDERS_STATUS_NOTE as i_WM_PURCHASE_ORDERS_STATUS_NOTE", 
	"SQ_Shortcut_to_WM_PURCHASE_ORDERS_STATUS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_PURCHASE_ORDERS_STATUS_temp = JNR_WM_PURCHASE_ORDERS_STATUS.toDF(*["JNR_WM_PURCHASE_ORDERS_STATUS___" + col for col in JNR_WM_PURCHASE_ORDERS_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_PURCHASE_ORDERS_STATUS_temp.selectExpr( 
	"JNR_WM_PURCHASE_ORDERS_STATUS___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___PURCHASE_ORDERS_STATUS as PURCHASE_ORDERS_STATUS", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___DESCRIPTION as DESCRIPTION", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___NOTE as NOTE", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___i_WM_PURCHASE_ORDERS_STATUS as i_WM_PURCHASE_ORDERS_STATUS", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___i_WM_PURCHASE_ORDERS_STATUS_DESC as i_WM_PURCHASE_ORDERS_STATUS_DESC", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___i_WM_PURCHASE_ORDERS_STATUS_NOTE as i_WM_PURCHASE_ORDERS_STATUS_NOTE", 
	"JNR_WM_PURCHASE_ORDERS_STATUS___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_PURCHASE_ORDERS_STATUS IS NULL OR (i_WM_PURCHASE_ORDERS_STATUS IS NOT NULL AND (COALESCE(DESCRIPTION, '') != COALESCE(i_WM_PURCHASE_ORDERS_STATUS_DESC, '') OR COALESCE(NOTE, '') != COALESCE(i_WM_PURCHASE_ORDERS_STATUS_NOTE, '')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___PURCHASE_ORDERS_STATUS as PURCHASE_ORDERS_STATUS", 
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_RECORDS___NOTE as NOTE", 
	"FIL_UNCHANGED_RECORDS___i_WM_PURCHASE_ORDERS_STATUS as i_WM_PURCHASE_ORDERS_STATUS", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_PURCHASE_ORDERS_STATUS IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPDATE_VALIDATOR_temp.selectExpr( 
	"EXP_UPDATE_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPDATE_VALIDATOR___PURCHASE_ORDERS_STATUS as PURCHASE_ORDERS_STATUS", 
	"EXP_UPDATE_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_UPDATE_VALIDATOR___NOTE as NOTE", 
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPDATE_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_PURCHASE_ORDERS_STATUS1, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_WM_PURCHASE_ORDERS_STATUS1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(PURCHASE_ORDERS_STATUS AS SMALLINT) as WM_PURCHASE_ORDERS_STATUS",
	"CAST(DESCRIPTION AS STRING) as WM_PURCHASE_ORDERS_STATUS_DESC",
	"CAST(NOTE AS STRING) as WM_PURCHASE_ORDERS_STATUS_NOTE",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_PURCHASE_ORDERS_STATUS = target.WM_PURCHASE_ORDERS_STATUS"""
  # refined_perf_table = "WM_PURCHASE_ORDERS_STATUS"
  executeMerge(Shortcut_to_WM_PURCHASE_ORDERS_STATUS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_PURCHASE_ORDERS_STATUS", "WM_PURCHASE_ORDERS_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_PURCHASE_ORDERS_STATUS", "WM_PURCHASE_ORDERS_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	