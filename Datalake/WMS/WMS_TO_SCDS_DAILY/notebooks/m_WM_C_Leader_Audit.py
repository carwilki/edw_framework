#Code converted on 2023-06-20 18:38:22
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

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_C_LEADER_AUDIT, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_C_LEADER_AUDIT = spark.sql(f"""SELECT
WM_C_LEADER_AUDIT.LOCATION_ID,
WM_C_LEADER_AUDIT.WM_C_LEADER_AUDIT_ID,
WM_C_LEADER_AUDIT.WM_CREATE_TSTMP,
WM_C_LEADER_AUDIT.WM_MOD_TSTMP,
WM_C_LEADER_AUDIT.LOAD_TSTMP
FROM WM_C_LEADER_AUDIT
WHERE WM_C_LEADER_AUDIT_ID IN (SELECT C_LEADER_AUDIT_ID FROM WM_C_LEADER_AUDIT_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE = spark.sql(f"""SELECT
WM_C_LEADER_AUDIT_PRE.DC_NBR,
WM_C_LEADER_AUDIT_PRE.C_LEADER_AUDIT_ID,
WM_C_LEADER_AUDIT_PRE.LEADER_USER_ID,
WM_C_LEADER_AUDIT_PRE.PICKER_USER_ID,
WM_C_LEADER_AUDIT_PRE.STATUS,
WM_C_LEADER_AUDIT_PRE.ITEM_NAME,
WM_C_LEADER_AUDIT_PRE.LPN,
WM_C_LEADER_AUDIT_PRE.EXPECTED_QTY,
WM_C_LEADER_AUDIT_PRE.ACTUAL_QTY,
WM_C_LEADER_AUDIT_PRE.CREATE_DATE_TIME,
WM_C_LEADER_AUDIT_PRE.MOD_DATE_TIME
FROM WM_C_LEADER_AUDIT_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE_temp = SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE.toDF(*["SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___" + col for col in SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___C_LEADER_AUDIT_ID as C_LEADER_AUDIT_ID", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___LEADER_USER_ID as LEADER_USER_ID", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___PICKER_USER_ID as PICKER_USER_ID", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___STATUS as STATUS", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___ITEM_NAME as ITEM_NAME", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___LPN as LPN", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___EXPECTED_QTY as EXPECTED_QTY", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___ACTUAL_QTY as ACTUAL_QTY", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT_PRE___MOD_DATE_TIME as MOD_DATE_TIME" \
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 13

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_C_LEADER_AUDIT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_C_LEADER_AUDIT_temp = SQ_Shortcut_to_WM_C_LEADER_AUDIT.toDF(*["SQ_Shortcut_to_WM_C_LEADER_AUDIT___" + col for col in SQ_Shortcut_to_WM_C_LEADER_AUDIT.columns])

JNR_WM_C_LEADER_AUDIT = SQ_Shortcut_to_WM_C_LEADER_AUDIT_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_C_LEADER_AUDIT_temp.SQ_Shortcut_to_WM_C_LEADER_AUDIT___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_C_LEADER_AUDIT_temp.SQ_Shortcut_to_WM_C_LEADER_AUDIT___WM_C_LEADER_AUDIT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___C_LEADER_AUDIT_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___C_LEADER_AUDIT_ID as C_LEADER_AUDIT_ID", \
	"JNR_SITE_PROFILE___LEADER_USER_ID as LEADER_USER_ID", \
	"JNR_SITE_PROFILE___PICKER_USER_ID as PICKER_USER_ID", \
	"JNR_SITE_PROFILE___STATUS as STATUS", \
	"JNR_SITE_PROFILE___ITEM_NAME as ITEM_NAME", \
	"JNR_SITE_PROFILE___LPN as LPN", \
	"JNR_SITE_PROFILE___EXPECTED_QTY as EXPECTED_QTY", \
	"JNR_SITE_PROFILE___ACTUAL_QTY as ACTUAL_QTY", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT___LOCATION_ID as i_LOCATION_ID1", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT___WM_C_LEADER_AUDIT_ID as i_WM_C_LEADER_AUDIT_ID", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT___WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_C_LEADER_AUDIT___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_C_LEADER_AUDIT_temp = JNR_WM_C_LEADER_AUDIT.toDF(*["JNR_WM_C_LEADER_AUDIT___" + col for col in JNR_WM_C_LEADER_AUDIT.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_C_LEADER_AUDIT_temp.selectExpr( \
	"JNR_WM_C_LEADER_AUDIT___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_C_LEADER_AUDIT___C_LEADER_AUDIT_ID as C_LEADER_AUDIT_ID", \
	"JNR_WM_C_LEADER_AUDIT___LEADER_USER_ID as LEADER_USER_ID", \
	"JNR_WM_C_LEADER_AUDIT___PICKER_USER_ID as PICKER_USER_ID", \
	"JNR_WM_C_LEADER_AUDIT___STATUS as STATUS", \
	"JNR_WM_C_LEADER_AUDIT___ITEM_NAME as ITEM_NAME", \
	"JNR_WM_C_LEADER_AUDIT___LPN as LPN", \
	"JNR_WM_C_LEADER_AUDIT___EXPECTED_QTY as EXPECTED_QTY", \
	"JNR_WM_C_LEADER_AUDIT___ACTUAL_QTY as ACTUAL_QTY", \
	"JNR_WM_C_LEADER_AUDIT___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_C_LEADER_AUDIT___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_C_LEADER_AUDIT___i_WM_C_LEADER_AUDIT_ID as i_WM_C_LEADER_AUDIT_ID", \
	"JNR_WM_C_LEADER_AUDIT___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", \
	"JNR_WM_C_LEADER_AUDIT___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", \
	"JNR_WM_C_LEADER_AUDIT___i_LOAD_TSTMP as i_LOAD_TSTMP")\
    .filter("i_WM_C_LEADER_AUDIT_ID is Null OR (  i_WM_C_LEADER_AUDIT_ID is NOT Null AND\
             ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___C_LEADER_AUDIT_ID as C_LEADER_AUDIT_ID", \
	"FIL_UNCHANGED_RECORDS___LEADER_USER_ID as LEADER_USER_ID", \
	"FIL_UNCHANGED_RECORDS___PICKER_USER_ID as PICKER_USER_ID", \
	"FIL_UNCHANGED_RECORDS___STATUS as STATUS", \
	"FIL_UNCHANGED_RECORDS___ITEM_NAME as ITEM_NAME", \
	"FIL_UNCHANGED_RECORDS___LPN as LPN", \
	"FIL_UNCHANGED_RECORDS___EXPECTED_QTY as EXPECTED_QTY", \
	"FIL_UNCHANGED_RECORDS___ACTUAL_QTY as ACTUAL_QTY", \
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_WM_C_LEADER_AUDIT_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( \
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_OUTPUT_VALIDATOR___C_LEADER_AUDIT_ID as C_LEADER_AUDIT_ID", \
	"EXP_OUTPUT_VALIDATOR___LEADER_USER_ID as LEADER_USER_ID", \
	"EXP_OUTPUT_VALIDATOR___PICKER_USER_ID as PICKER_USER_ID", \
	"EXP_OUTPUT_VALIDATOR___STATUS as STATUS", \
	"EXP_OUTPUT_VALIDATOR___ITEM_NAME as ITEM_NAME", \
	"EXP_OUTPUT_VALIDATOR___LPN as LPN", \
	"EXP_OUTPUT_VALIDATOR___EXPECTED_QTY as EXPECTED_QTY", \
	"EXP_OUTPUT_VALIDATOR___ACTUAL_QTY as ACTUAL_QTY", \
	"EXP_OUTPUT_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_OUTPUT_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)) .when(EXP_OUTPUT_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_C_LEADER_AUDIT1, type TARGET 
# COLUMN COUNT: 13

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_C_LEADER_AUDIT_ID = target.WM_C_LEADER_AUDIT_ID"""
  refined_perf_table = "WM_C_LEADER_AUDIT"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_C_LEADER_AUDIT", "WM_C_LEADER_AUDIT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_C_LEADER_AUDIT", "WM_C_LEADER_AUDIT","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	