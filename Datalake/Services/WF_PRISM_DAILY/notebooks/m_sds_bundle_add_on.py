#Code converted on 2023-07-21 10:23:34
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE = spark.sql(f"""SELECT
ID,
OWNER_ID,
IS_DELETED,
NAME,
CURRENCY_ISO_CODE,
CREATED_DATE,
CREATED_BY_ID,
LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID,
SYSTEM_MOD_STAMP,
LAST_ACTIVITY_DATE,
PSVC_ADDON_C,
PSVC_BULK_ADDON_C,
PSVC_BUNDLE_C,
PSVC_ORIGINAL_UNIT_PRICE_C
FROM {raw}.SDS_PSVC_BUNDLE_ADD_ON_C_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_BUNDLE_ADD_ON, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SDS_BUNDLE_ADD_ON = spark.sql(f"""SELECT
SDS_BUNDLE_ADD_ON_ID,
SDS_SYSTEM_MODIFIED_TSTMP,
SDS_LAST_MODIFIED_TSTMP,
SDS_CREATED_TSTMP,
LOAD_TSTMP
FROM {legacy}.SDS_BUNDLE_ADD_ON""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_BUNDLE_ADD_ON, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE_temp = SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE.toDF(*["SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___" + col for col in SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE.columns])
SQ_Shortcut_to_SDS_BUNDLE_ADD_ON_temp = SQ_Shortcut_to_SDS_BUNDLE_ADD_ON.toDF(*["SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___" + col for col in SQ_Shortcut_to_SDS_BUNDLE_ADD_ON.columns])

JNR_SDS_BUNDLE_ADD_ON = SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE_temp.join(SQ_Shortcut_to_SDS_BUNDLE_ADD_ON_temp,[SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE_temp.SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___ID == SQ_Shortcut_to_SDS_BUNDLE_ADD_ON_temp.SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___SDS_BUNDLE_ADD_ON_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___ID as ID",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___OWNER_ID as OWNER_ID",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___IS_DELETED as IS_DELETED",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___NAME as NAME",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___CREATED_DATE as CREATED_DATE",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___CREATED_BY_ID as CREATED_BY_ID",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___LAST_ACTIVITY_DATE as LAST_ACTIVITY_DATE",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___PSVC_ADDON_C as PSVC_ADDON_C",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___PSVC_BULK_ADDON_C as PSVC_BULK_ADDON_C",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___PSVC_BUNDLE_C as PSVC_BUNDLE_C",
	"SQ_Shortcut_to_SDS_PSVC_BUNDLE_ADD_ON_C_PRE___PSVC_ORIGINAL_UNIT_PRICE_C as PSVC_ORIGINAL_UNIT_PRICE_C",
	"SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___SDS_BUNDLE_ADD_ON_ID as i_SDS_BUNDLE_ADD_ON_ID",
	"SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___SDS_SYSTEM_MODIFIED_TSTMP as i_SDS_SYSTEM_MODIFIED_TSTMP",
	"SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___SDS_LAST_MODIFIED_TSTMP as i_SDS_LAST_MODIFIED_TSTMP",
	"SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"SQ_Shortcut_to_SDS_BUNDLE_ADD_ON___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_SDS_BUNDLE_ADD_ON, type FILTER 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_BUNDLE_ADD_ON_temp = JNR_SDS_BUNDLE_ADD_ON.toDF(*["JNR_SDS_BUNDLE_ADD_ON___" + col for col in JNR_SDS_BUNDLE_ADD_ON.columns])

FIL_SDS_BUNDLE_ADD_ON = JNR_SDS_BUNDLE_ADD_ON_temp.selectExpr(
	"JNR_SDS_BUNDLE_ADD_ON___ID as ID",
	"JNR_SDS_BUNDLE_ADD_ON___OWNER_ID as OWNER_ID",
	"JNR_SDS_BUNDLE_ADD_ON___IS_DELETED as IS_DELETED",
	"JNR_SDS_BUNDLE_ADD_ON___NAME as NAME",
	"JNR_SDS_BUNDLE_ADD_ON___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"JNR_SDS_BUNDLE_ADD_ON___CREATED_DATE as CREATED_DATE",
	"JNR_SDS_BUNDLE_ADD_ON___CREATED_BY_ID as CREATED_BY_ID",
	"JNR_SDS_BUNDLE_ADD_ON___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"JNR_SDS_BUNDLE_ADD_ON___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"JNR_SDS_BUNDLE_ADD_ON___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"JNR_SDS_BUNDLE_ADD_ON___LAST_ACTIVITY_DATE as LAST_ACTIVITY_DATE",
	"JNR_SDS_BUNDLE_ADD_ON___PSVC_ADDON_C as PSVC_ADDON_C",
	"JNR_SDS_BUNDLE_ADD_ON___PSVC_BULK_ADDON_C as PSVC_BULK_ADDON_C",
	"JNR_SDS_BUNDLE_ADD_ON___PSVC_BUNDLE_C as PSVC_BUNDLE_C",
	"JNR_SDS_BUNDLE_ADD_ON___PSVC_ORIGINAL_UNIT_PRICE_C as PSVC_ORIGINAL_UNIT_PRICE_C",
	"JNR_SDS_BUNDLE_ADD_ON___i_SDS_BUNDLE_ADD_ON_ID as i_SDS_BUNDLE_ADD_ON_ID",
	"JNR_SDS_BUNDLE_ADD_ON___i_SDS_SYSTEM_MODIFIED_TSTMP as i_SDS_SYSTEM_MODIFIED_TSTMP",
	"JNR_SDS_BUNDLE_ADD_ON___i_SDS_LAST_MODIFIED_TSTMP as i_SDS_LAST_MODIFIED_TSTMP",
	"JNR_SDS_BUNDLE_ADD_ON___i_SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"JNR_SDS_BUNDLE_ADD_ON___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_SDS_BUNDLE_ADD_ON_ID IS NULL OR ( i_SDS_BUNDLE_ADD_ON_ID IS NOT NULL AND ( i_SDS_SYSTEM_MODIFIED_TSTMP != SYSTEM_MOD_STAMP OR i_SDS_CREATED_TSTMP != CREATED_DATE OR i_SDS_LAST_MODIFIED_TSTMP != LAST_MODIFIED_DATE ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_BUNDLE_ADD_ON, type EXPRESSION 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
FIL_SDS_BUNDLE_ADD_ON_temp = FIL_SDS_BUNDLE_ADD_ON.toDF(*["FIL_SDS_BUNDLE_ADD_ON___" + col for col in FIL_SDS_BUNDLE_ADD_ON.columns])

EXP_SDS_BUNDLE_ADD_ON = FIL_SDS_BUNDLE_ADD_ON_temp.selectExpr(
	"FIL_SDS_BUNDLE_ADD_ON___sys_row_id as sys_row_id",
	"FIL_SDS_BUNDLE_ADD_ON___ID as ID",
	"FIL_SDS_BUNDLE_ADD_ON___OWNER_ID as OWNER_ID",
	"FIL_SDS_BUNDLE_ADD_ON___IS_DELETED as IS_DELETED",
	"FIL_SDS_BUNDLE_ADD_ON___NAME as NAME",
	"FIL_SDS_BUNDLE_ADD_ON___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"FIL_SDS_BUNDLE_ADD_ON___CREATED_DATE as CREATED_DATE",
	"FIL_SDS_BUNDLE_ADD_ON___CREATED_BY_ID as CREATED_BY_ID",
	"FIL_SDS_BUNDLE_ADD_ON___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"FIL_SDS_BUNDLE_ADD_ON___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"FIL_SDS_BUNDLE_ADD_ON___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"FIL_SDS_BUNDLE_ADD_ON___LAST_ACTIVITY_DATE as LAST_ACTIVITY_DATE",
	"FIL_SDS_BUNDLE_ADD_ON___PSVC_ADDON_C as PSVC_ADDON_C",
	"FIL_SDS_BUNDLE_ADD_ON___PSVC_BULK_ADDON_C as PSVC_BULK_ADDON_C",
	"FIL_SDS_BUNDLE_ADD_ON___PSVC_BUNDLE_C as PSVC_BUNDLE_C",
	"FIL_SDS_BUNDLE_ADD_ON___PSVC_ORIGINAL_UNIT_PRICE_C as PSVC_ORIGINAL_UNIT_PRICE_C",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_SDS_BUNDLE_ADD_ON___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_SDS_BUNDLE_ADD_ON___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_SDS_BUNDLE_ADD_ON___i_SDS_BUNDLE_ADD_ON_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------
# Processing node UPD_SDS_BUNDLE_ADD_ON, type UPDATE_STRATEGY 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_BUNDLE_ADD_ON_temp = EXP_SDS_BUNDLE_ADD_ON.toDF(*["EXP_SDS_BUNDLE_ADD_ON___" + col for col in EXP_SDS_BUNDLE_ADD_ON.columns])

UPD_SDS_BUNDLE_ADD_ON = EXP_SDS_BUNDLE_ADD_ON_temp.selectExpr(
	"EXP_SDS_BUNDLE_ADD_ON___ID as ID",
	"EXP_SDS_BUNDLE_ADD_ON___OWNER_ID as OWNER_ID",
	"EXP_SDS_BUNDLE_ADD_ON___IS_DELETED as IS_DELETED",
	"EXP_SDS_BUNDLE_ADD_ON___NAME as NAME",
	"EXP_SDS_BUNDLE_ADD_ON___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"EXP_SDS_BUNDLE_ADD_ON___CREATED_DATE as CREATED_DATE",
	"EXP_SDS_BUNDLE_ADD_ON___CREATED_BY_ID as CREATED_BY_ID",
	"EXP_SDS_BUNDLE_ADD_ON___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"EXP_SDS_BUNDLE_ADD_ON___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"EXP_SDS_BUNDLE_ADD_ON___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"EXP_SDS_BUNDLE_ADD_ON___LAST_ACTIVITY_DATE as LAST_ACTIVITY_DATE",
	"EXP_SDS_BUNDLE_ADD_ON___PSVC_ADDON_C as PSVC_ADDON_C",
	"EXP_SDS_BUNDLE_ADD_ON___PSVC_BULK_ADDON_C as PSVC_BULK_ADDON_C",
	"EXP_SDS_BUNDLE_ADD_ON___PSVC_BUNDLE_C as PSVC_BUNDLE_C",
	"EXP_SDS_BUNDLE_ADD_ON___PSVC_ORIGINAL_UNIT_PRICE_C as PSVC_ORIGINAL_UNIT_PRICE_C",
	"EXP_SDS_BUNDLE_ADD_ON___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_BUNDLE_ADD_ON___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_BUNDLE_ADD_ON___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR")\
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)) , lit(0)) .when(col('o_UPDATE_VALIDATOR') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_SDS_BUNDLE_ADD_ON1, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_SDS_BUNDLE_ADD_ON1 = UPD_SDS_BUNDLE_ADD_ON.selectExpr(
	"CAST(ID AS STRING) as SDS_BUNDLE_ADD_ON_ID",
	"CAST(NAME AS STRING) as SDS_BUNDLE_ADD_ON_NAME",
	"CAST(PSVC_BULK_ADDON_C AS STRING) as SDS_BULK_ADD_ON_PRODUCT_ID",
	"CAST(PSVC_ADDON_C AS STRING) as SDS_ADD_ON_PRODUCT_ID",
	"CAST(PSVC_BUNDLE_C AS STRING) as SDS_BUNDLE_ID",
	"CAST(PSVC_ORIGINAL_UNIT_PRICE_C AS DECIMAL(10,2)) as ORIG_UNIT_PRICE_AMT",
	"CAST(OWNER_ID AS STRING) as SDS_OWNER_ID",
	"CAST(CURRENCY_ISO_CODE AS STRING) as CURRENCY_ISO_CD",
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
	"CAST(LAST_ACTIVITY_DATE AS TIMESTAMP) as SDS_LAST_ACTIVITY_TSTMP",
	"CAST(SYSTEM_MOD_STAMP AS TIMESTAMP) as SDS_SYSTEM_MODIFIED_TSTMP",
	"CAST(LAST_MODIFIED_DATE AS TIMESTAMP) as SDS_LAST_MODIFIED_TSTMP",
	"CAST(LAST_MODIFIED_BY_ID AS STRING) as SDS_LAST_MODIFIED_BY_ID",
	"CAST(CREATED_DATE AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(CREATED_BY_ID AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_SDS_BUNDLE_ADD_ON.pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.SDS_BUNDLE_ADD_ON_ID = target.SDS_BUNDLE_ADD_ON_ID"""
  refined_perf_table = "{legacy}.SDS_BUNDLE_ADD_ON"
  executeMerge(Shortcut_to_SDS_BUNDLE_ADD_ON1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SDS_BUNDLE_ADD_ON", "SDS_BUNDLE_ADD_ON", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SDS_BUNDLE_ADD_ON", "SDS_BUNDLE_ADD_ON","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	