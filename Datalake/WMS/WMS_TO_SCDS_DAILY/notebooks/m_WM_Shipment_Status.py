#Code converted on 2023-06-22 20:58:17
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
# Processing node SQ_Shortcut_to_WM_SHIPMENT_STATUS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_SHIPMENT_STATUS = spark.sql(f"""SELECT
WM_SHIPMENT_STATUS.LOCATION_ID,
WM_SHIPMENT_STATUS.WM_SHIPMENT_STATUS,
WM_SHIPMENT_STATUS.WM_SHIPMENT_STATUS_DESC,
WM_SHIPMENT_STATUS.WM_SHIPMENT_STATUS_SHORT_DESC,
WM_SHIPMENT_STATUS.LOAD_TSTMP
FROM WM_SHIPMENT_STATUS
WHERE WM_SHIPMENT_STATUS IN (SELECT SHIPMENT_STATUS FROM WM_SHIPMENT_STATUS_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE = spark.sql(f"""SELECT
WM_SHIPMENT_STATUS_PRE.DC_NBR,
WM_SHIPMENT_STATUS_PRE.SHIPMENT_STATUS,
WM_SHIPMENT_STATUS_PRE.DESCRIPTION,
WM_SHIPMENT_STATUS_PRE.SHORT_DESC,
WM_SHIPMENT_STATUS_PRE.LOAD_TSTMP
FROM WM_SHIPMENT_STATUS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE_temp = SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE.toDF(*["SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___" + col for col in SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE.columns])

EXP_TRANS = SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___sys_row_id as sys_row_id", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___DC_NBR as DC_NBR", 
	"cast(SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___SHIPMENT_STATUS as SHIPMENT_STATUS", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___SHORT_DESC as SHORT_DESC", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 6

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_TRANS,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_TRANS.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_SHIPMENT_STATUS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_SHIPMENT_STATUS_temp = SQ_Shortcut_to_WM_SHIPMENT_STATUS.toDF(*["SQ_Shortcut_to_WM_SHIPMENT_STATUS___" + col for col in SQ_Shortcut_to_WM_SHIPMENT_STATUS.columns])

JNR_WM_SHIPMENT_STATUS = SQ_Shortcut_to_WM_SHIPMENT_STATUS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_SHIPMENT_STATUS_temp.SQ_Shortcut_to_WM_SHIPMENT_STATUS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_SHIPMENT_STATUS_temp.SQ_Shortcut_to_WM_SHIPMENT_STATUS___WM_SHIPMENT_STATUS == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SHIPMENT_STATUS],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___SHIPMENT_STATUS as SHIPMENT_STATUS", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___SHORT_DESC as SHORT_DESC", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS___WM_SHIPMENT_STATUS as i_WM_SHIPMENT_STATUS", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS___WM_SHIPMENT_STATUS_DESC as i_WM_SHIPMENT_STATUS_DESC", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS___WM_SHIPMENT_STATUS_SHORT_DESC as i_WM_SHIPMENT_STATUS_SHORT_DESC", 
	"SQ_Shortcut_to_WM_SHIPMENT_STATUS___LOAD_TSTMP as i_LOAD_TSTMP"
)

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_SHIPMENT_STATUS_temp = JNR_WM_SHIPMENT_STATUS.toDF(*["JNR_WM_SHIPMENT_STATUS___" + col for col in JNR_WM_SHIPMENT_STATUS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_SHIPMENT_STATUS_temp.selectExpr( 
	"JNR_WM_SHIPMENT_STATUS___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_SHIPMENT_STATUS___SHIPMENT_STATUS as SHIPMENT_STATUS", 
	"JNR_WM_SHIPMENT_STATUS___DESCRIPTION as DESCRIPTION", 
	"JNR_WM_SHIPMENT_STATUS___SHORT_DESC as SHORT_DESC", 
	"JNR_WM_SHIPMENT_STATUS___i_WM_SHIPMENT_STATUS as i_WM_SHIPMENT_STATUS", 
	"JNR_WM_SHIPMENT_STATUS___i_WM_SHIPMENT_STATUS_DESC as i_WM_SHIPMENT_STATUS_DESC", 
	"JNR_WM_SHIPMENT_STATUS___i_WM_SHIPMENT_STATUS_SHORT_DESC as i_WM_SHIPMENT_STATUS_SHORT_DESC", 
	"JNR_WM_SHIPMENT_STATUS___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_SHIPMENT_STATUS IS NULL OR (NOT i_WM_SHIPMENT_STATUS IS NULL AND (COALESCE(DESCRIPTION, '') != COALESCE(i_WM_SHIPMENT_STATUS_DESC, '')) OR (COALESCE(SHORT_DESC, '') != COALESCE(i_WM_SHIPMENT_STATUS_SHORT_DESC, '')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___SHIPMENT_STATUS as SHIPMENT_STATUS", 
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_RECORDS___SHORT_DESC as SHORT_DESC", 
	"FIL_UNCHANGED_RECORDS___i_WM_SHIPMENT_STATUS as i_WM_SHIPMENT_STATUS", 
	"FIL_UNCHANGED_RECORDS___i_WM_SHIPMENT_STATUS_DESC as i_WM_SHIPMENT_STATUS_DESC", 
	"FIL_UNCHANGED_RECORDS___i_WM_SHIPMENT_STATUS_SHORT_DESC as i_WM_SHIPMENT_STATUS_SHORT_DESC", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF (FIL_UNCHANGED_RECORDS___i_WM_SHIPMENT_STATUS IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPDATE_VALIDATOR_temp.selectExpr( 
	"EXP_UPDATE_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPDATE_VALIDATOR___SHIPMENT_STATUS as SHIPMENT_STATUS", 
	"EXP_UPDATE_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_UPDATE_VALIDATOR___SHORT_DESC as SHORT_DESC", 
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPDATE_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR").withColumn('pyspark_data_action', when(EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)).when(EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_SHIPMENT_STATUS1, type TARGET 
# COLUMN COUNT: 6

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SHIPMENT_STATUS = target.WM_SHIPMENT_STATUS"""
  refined_perf_table = "WM_SHIPMENT_STATUS"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_SHIPMENT_STATUS", "WM_SHIPMENT_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_SHIPMENT_STATUS", "WM_SHIPMENT_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	