#Code converted on 2023-06-22 20:59:53
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
refined_perf_table = f"{refine}.WM_SLOT_ITEM_SCORE"
raw_perf_table = f"{raw}.WM_SLOT_ITEM_SCORE_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SLOT_ITEM_SCORE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_SLOT_ITEM_SCORE = spark.sql(f"""SELECT
LOCATION_ID,
WM_SLOT_ITEM_SCORE_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_SLOT_ITEM_SCORE_ID IN (SELECT SLOT_ITEM_SCORE_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE = spark.sql(f"""SELECT
DC_NBR,
SLOT_ITEM_SCORE_ID,
SLOTITEM_ID,
CNSTR_ID,
SCORE,
CREATE_DATE_TIME,
MOD_DATE_TIME,
MOD_USER,
SEQ_CNSTR_VIOLATION,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE_temp = SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE.toDF(*["SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___" + col for col in SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___sys_row_id as sys_row_id", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___DC_NBR as DC_NBR", 
	"cast(SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___SLOT_ITEM_SCORE_ID as SLOT_ITEM_SCORE_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___SLOTITEM_ID as SLOTITEM_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___CNSTR_ID as CNSTR_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___SCORE as SCORE", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___MOD_USER as MOD_USER", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___SEQ_CNSTR_VIOLATION as SEQ_CNSTR_VIOLATION", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 12

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXPTRANS,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXPTRANS.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_SLOT_ITEM_SCORE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_temp = SQ_Shortcut_to_WM_SLOT_ITEM_SCORE.toDF(*["SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___" + col for col in SQ_Shortcut_to_WM_SLOT_ITEM_SCORE.columns])

JNR_WM_SLOT_ITEM_SCORE = SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_temp.SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_SLOT_ITEM_SCORE_temp.SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___WM_SLOT_ITEM_SCORE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SLOT_ITEM_SCORE_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___SLOT_ITEM_SCORE_ID as SLOT_ITEM_SCORE_ID", 
	"JNR_SITE_PROFILE___SLOTITEM_ID as SLOTITEM_ID", 
	"JNR_SITE_PROFILE___CNSTR_ID as CNSTR_ID", 
	"JNR_SITE_PROFILE___SCORE as SCORE", 
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_SITE_PROFILE___MOD_USER as MOD_USER", 
	"JNR_SITE_PROFILE___SEQ_CNSTR_VIOLATION as SEQ_CNSTR_VIOLATION", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___WM_SLOT_ITEM_SCORE_ID as i_WM_SLOT_ITEM_SCORE_ID", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"SQ_Shortcut_to_WM_SLOT_ITEM_SCORE___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_SLOT_ITEM_SCORE_temp = JNR_WM_SLOT_ITEM_SCORE.toDF(*["JNR_WM_SLOT_ITEM_SCORE___" + col for col in JNR_WM_SLOT_ITEM_SCORE.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_SLOT_ITEM_SCORE_temp.selectExpr( 
	"JNR_WM_SLOT_ITEM_SCORE___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_SLOT_ITEM_SCORE___SLOT_ITEM_SCORE_ID as SLOT_ITEM_SCORE_ID", 
	"JNR_WM_SLOT_ITEM_SCORE___SLOTITEM_ID as SLOTITEM_ID", 
	"JNR_WM_SLOT_ITEM_SCORE___CNSTR_ID as CNSTR_ID", 
	"JNR_WM_SLOT_ITEM_SCORE___SCORE as SCORE", 
	"JNR_WM_SLOT_ITEM_SCORE___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"JNR_WM_SLOT_ITEM_SCORE___MOD_DATE_TIME as MOD_DATE_TIME", 
	"JNR_WM_SLOT_ITEM_SCORE___MOD_USER as MOD_USER", 
	"JNR_WM_SLOT_ITEM_SCORE___SEQ_CNSTR_VIOLATION as SEQ_CNSTR_VIOLATION", 
	"JNR_WM_SLOT_ITEM_SCORE___i_WM_SLOT_ITEM_SCORE_ID as i_WM_SLOT_ITEM_SCORE_ID", 
	"JNR_WM_SLOT_ITEM_SCORE___i_WM_CREATE_TSTMP as i_WM_CREATE_TSTMP", 
	"JNR_WM_SLOT_ITEM_SCORE___i_WM_MOD_TSTMP as i_WM_MOD_TSTMP", 
	"JNR_WM_SLOT_ITEM_SCORE___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_SLOT_ITEM_SCORE_ID IS NULL OR (NOT i_WM_SLOT_ITEM_SCORE_ID IS NULL AND (COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_CREATE_TSTMP, date'1900-01-01')) OR (COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(i_WM_MOD_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___SLOT_ITEM_SCORE_ID as SLOT_ITEM_SCORE_ID", 
	"FIL_UNCHANGED_RECORDS___SLOTITEM_ID as SLOTITEM_ID", 
	"FIL_UNCHANGED_RECORDS___CNSTR_ID as CNSTR_ID", 
	"FIL_UNCHANGED_RECORDS___SCORE as SCORE", 
	"FIL_UNCHANGED_RECORDS___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_DATE_TIME as MOD_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___MOD_USER as MOD_USER", 
	"FIL_UNCHANGED_RECORDS___SEQ_CNSTR_VIOLATION as SEQ_CNSTR_VIOLATION", 
	"FIL_UNCHANGED_RECORDS___i_WM_SLOT_ITEM_SCORE_ID as i_WM_SLOT_ITEM_SCORE_ID", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_SLOT_ITEM_SCORE_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPDATE_VALIDATOR_temp.selectExpr( 
	"EXP_UPDATE_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPDATE_VALIDATOR___SLOT_ITEM_SCORE_ID as SLOT_ITEM_SCORE_ID", 
	"EXP_UPDATE_VALIDATOR___SLOTITEM_ID as SLOTITEM_ID", 
	"EXP_UPDATE_VALIDATOR___CNSTR_ID as CNSTR_ID", 
	"EXP_UPDATE_VALIDATOR___SCORE as SCORE", 
	"EXP_UPDATE_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", 
	"EXP_UPDATE_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", 
	"EXP_UPDATE_VALIDATOR___MOD_USER as MOD_USER", 
	"EXP_UPDATE_VALIDATOR___SEQ_CNSTR_VIOLATION as SEQ_CNSTR_VIOLATION", 
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPDATE_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPDATE_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)),lit(0)).when(EXP_UPDATE_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_SLOT_ITEM_SCORE1, type TARGET 
# COLUMN COUNT: 11


Shortcut_to_WM_SLOT_ITEM_SCORE1 = UPD_INS_UPD.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(SLOT_ITEM_SCORE_ID AS BIGINT) as WM_SLOT_ITEM_SCORE_ID", 
	"CAST(SLOTITEM_ID AS BIGINT) as WM_SLOT_ITEM_ID", 
	"CAST(CNSTR_ID AS BIGINT) as WM_CNSTR_ID", 
	"CAST(SEQ_CNSTR_VIOLATION AS BIGINT) as WM_SEQ_CNSTR_VIOLATION", 
	"CAST(SCORE AS BIGINT) as SCORE", 
	"CAST(MOD_USER AS STRING) as WM_MOD_USER", 
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP", 
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SLOT_ITEM_SCORE_ID = target.WM_SLOT_ITEM_SCORE_ID"""
  # refined_perf_table = "WM_SLOT_ITEM_SCORE"
  executeMerge(Shortcut_to_WM_SLOT_ITEM_SCORE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_SLOT_ITEM_SCORE", "WM_SLOT_ITEM_SCORE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_SLOT_ITEM_SCORE", "WM_SLOT_ITEM_SCORE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	