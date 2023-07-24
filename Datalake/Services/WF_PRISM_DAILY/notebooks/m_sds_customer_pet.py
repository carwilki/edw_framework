#Code converted on 2023-07-21 14:34:57
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
# Processing node SQ_Shortcut_to_SDS_CUSTOMER_PET, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SDS_CUSTOMER_PET = spark.sql(f"""SELECT
SDS_CUSTOMER_PET_ID,
SDS_CREATED_TSTMP,
SDS_SYSTEM_MODIFIED_TSTMP,
SDS_LAST_MODIFIED_TSTMP,
LOAD_TSTMP
FROM {legacy}.SDS_CUSTOMER_PET""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE = spark.sql(f"""SELECT
ID,
IS_DELETED,
NAME,
CURRENCY_ISO_CODE,
CREATED_DATE,
CREATED_BY_ID,
LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID,
SYSTEM_MOD_STAMP,
PSVC_CUSTOMER_C,
PSVC_PET_C
FROM {raw}.SDS_PSVC_CUSTOMER_PET_C_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_CUSTOMER_PET, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_CUSTOMER_PET_temp = SQ_Shortcut_to_SDS_CUSTOMER_PET.toDF(*["SQ_Shortcut_to_SDS_CUSTOMER_PET___" + col for col in SQ_Shortcut_to_SDS_CUSTOMER_PET.columns])
SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE_temp = SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE.toDF(*["SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___" + col for col in SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE.columns])

JNR_SDS_CUSTOMER_PET = SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE_temp.join(SQ_Shortcut_to_SDS_CUSTOMER_PET_temp,[SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE_temp.SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___ID == SQ_Shortcut_to_SDS_CUSTOMER_PET_temp.SQ_Shortcut_to_SDS_CUSTOMER_PET___SDS_CUSTOMER_PET_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_SDS_CUSTOMER_PET___SDS_CUSTOMER_PET_ID as i_SDS_CUSTOMER_PET_ID",
	"SQ_Shortcut_to_SDS_CUSTOMER_PET___SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"SQ_Shortcut_to_SDS_CUSTOMER_PET___SDS_SYSTEM_MODIFIED_TSTMP as i_SDS_SYSTEM_MODIFIED_TSTMP",
	"SQ_Shortcut_to_SDS_CUSTOMER_PET___SDS_LAST_MODIFIED_TSTMP as i_SDS_LAST_MODIFIED_TSTMP",
	"SQ_Shortcut_to_SDS_CUSTOMER_PET___LOAD_TSTMP as i_LOAD_TSTMP",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___ID as ID",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___IS_DELETED as IS_DELETED",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___NAME as NAME",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___CREATED_DATE as CREATED_DATE",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___CREATED_BY_ID as CREATED_BY_ID",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___PSVC_CUSTOMER_C as PSVC_CUSTOMER_C",
	"SQ_Shortcut_to_SDS_PSVC_CUSTOMER_PET_C_PRE___PSVC_PET_C as PSVC_PET_C")

# COMMAND ----------
# Processing node FIL_SDS_CUSTOMER_PET, type FILTER 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_CUSTOMER_PET_temp = JNR_SDS_CUSTOMER_PET.toDF(*["JNR_SDS_CUSTOMER_PET___" + col for col in JNR_SDS_CUSTOMER_PET.columns])

FIL_SDS_CUSTOMER_PET = JNR_SDS_CUSTOMER_PET_temp.selectExpr(
	"JNR_SDS_CUSTOMER_PET___i_SDS_CUSTOMER_PET_ID as i_SDS_CUSTOMER_PET_ID",
	"JNR_SDS_CUSTOMER_PET___i_SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"JNR_SDS_CUSTOMER_PET___i_SDS_SYSTEM_MODIFIED_TSTMP as i_SDS_SYSTEM_MODIFIED_TSTMP",
	"JNR_SDS_CUSTOMER_PET___i_SDS_LAST_MODIFIED_TSTMP as i_SDS_LAST_MODIFIED_TSTMP",
	"JNR_SDS_CUSTOMER_PET___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"JNR_SDS_CUSTOMER_PET___ID as ID",
	"JNR_SDS_CUSTOMER_PET___IS_DELETED as IS_DELETED",
	"JNR_SDS_CUSTOMER_PET___NAME as NAME",
	"JNR_SDS_CUSTOMER_PET___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"JNR_SDS_CUSTOMER_PET___CREATED_DATE as CREATED_DATE",
	"JNR_SDS_CUSTOMER_PET___CREATED_BY_ID as CREATED_BY_ID",
	"JNR_SDS_CUSTOMER_PET___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"JNR_SDS_CUSTOMER_PET___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"JNR_SDS_CUSTOMER_PET___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"JNR_SDS_CUSTOMER_PET___PSVC_CUSTOMER_C as PSVC_CUSTOMER_C",
	"JNR_SDS_CUSTOMER_PET___PSVC_PET_C as PSVC_PET_C").filter("i_SDS_CUSTOMER_PET_ID IS NULL OR ( i_SDS_CUSTOMER_PET_ID IS NOT NULL AND ( i_SDS_SYSTEM_MODIFIED_TSTMP != SYSTEM_MOD_STAMP OR i_SDS_CREATED_TSTMP != CREATED_DATE OR i_SDS_LAST_MODIFIED_TSTMP != LAST_MODIFIED_DATE ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_CUSTOMER_PET, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FIL_SDS_CUSTOMER_PET_temp = FIL_SDS_CUSTOMER_PET.toDF(*["FIL_SDS_CUSTOMER_PET___" + col for col in FIL_SDS_CUSTOMER_PET.columns])

EXP_SDS_CUSTOMER_PET = FIL_SDS_CUSTOMER_PET_temp.selectExpr(
	"FIL_SDS_CUSTOMER_PET___i_SDS_CUSTOMER_PET_ID as i_SDS_CUSTOMER_PET_ID",
	"FIL_SDS_CUSTOMER_PET___i_SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"FIL_SDS_CUSTOMER_PET___i_SDS_SYSTEM_MODIFIED_TSTMP as SDS_SYSTEM_MODIFIED_TSTMP",
	"FIL_SDS_CUSTOMER_PET___i_SDS_LAST_MODIFIED_TSTMP as SDS_LAST_MODIFIED_TSTMP",
	"FIL_SDS_CUSTOMER_PET___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"FIL_SDS_CUSTOMER_PET___ID as ID",
	"FIL_SDS_CUSTOMER_PET___IS_DELETED as IS_DELETED",
	"FIL_SDS_CUSTOMER_PET___NAME as NAME",
	"FIL_SDS_CUSTOMER_PET___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"FIL_SDS_CUSTOMER_PET___CREATED_DATE as CREATED_DATE",
	"FIL_SDS_CUSTOMER_PET___CREATED_BY_ID as CREATED_BY_ID",
	"FIL_SDS_CUSTOMER_PET___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"FIL_SDS_CUSTOMER_PET___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"FIL_SDS_CUSTOMER_PET___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"FIL_SDS_CUSTOMER_PET___PSVC_CUSTOMER_C as PSVC_CUSTOMER_C",
	"FIL_SDS_CUSTOMER_PET___PSVC_PET_C as PSVC_PET_C").selectExpr(
	"FIL_SDS_CUSTOMER_PET___sys_row_id as sys_row_id",
	"FIL_SDS_CUSTOMER_PET___ID as ID",
	"FIL_SDS_CUSTOMER_PET___IS_DELETED as IS_DELETED",
	"FIL_SDS_CUSTOMER_PET___NAME as NAME",
	"FIL_SDS_CUSTOMER_PET___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"FIL_SDS_CUSTOMER_PET___CREATED_DATE as CREATED_DATE",
	"FIL_SDS_CUSTOMER_PET___CREATED_BY_ID as CREATED_BY_ID",
	"FIL_SDS_CUSTOMER_PET___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"FIL_SDS_CUSTOMER_PET___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"FIL_SDS_CUSTOMER_PET___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"FIL_SDS_CUSTOMER_PET___PSVC_CUSTOMER_C as PSVC_CUSTOMER_C",
	"FIL_SDS_CUSTOMER_PET___PSVC_PET_C as PSVC_PET_C",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_SDS_CUSTOMER_PET___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_SDS_CUSTOMER_PET___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_SDS_CUSTOMER_PET___i_SDS_CUSTOMER_PET_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------
# Processing node UPD_SDS_CUSTOMER_PET, type UPDATE_STRATEGY 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_CUSTOMER_PET_temp = EXP_SDS_CUSTOMER_PET.toDF(*["EXP_SDS_CUSTOMER_PET___" + col for col in EXP_SDS_CUSTOMER_PET.columns])

UPD_SDS_CUSTOMER_PET = EXP_SDS_CUSTOMER_PET_temp.selectExpr(
	"EXP_SDS_CUSTOMER_PET___ID as ID",
	"EXP_SDS_CUSTOMER_PET___IS_DELETED as IS_DELETED",
	"EXP_SDS_CUSTOMER_PET___NAME as NAME",
	"EXP_SDS_CUSTOMER_PET___CURRENCY_ISO_CODE as CURRENCY_ISO_CODE",
	"EXP_SDS_CUSTOMER_PET___CREATED_DATE as CREATED_DATE",
	"EXP_SDS_CUSTOMER_PET___CREATED_BY_ID as CREATED_BY_ID",
	"EXP_SDS_CUSTOMER_PET___LAST_MODIFIED_DATE as LAST_MODIFIED_DATE",
	"EXP_SDS_CUSTOMER_PET___LAST_MODIFIED_BY_ID as LAST_MODIFIED_BY_ID",
	"EXP_SDS_CUSTOMER_PET___SYSTEM_MOD_STAMP as SYSTEM_MOD_STAMP",
	"EXP_SDS_CUSTOMER_PET___PSVC_CUSTOMER_C as PSVC_CUSTOMER_C",
	"EXP_SDS_CUSTOMER_PET___PSVC_PET_C as PSVC_PET_C",
	"EXP_SDS_CUSTOMER_PET___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_CUSTOMER_PET___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_CUSTOMER_PET___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR")\
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)) , lit(0)) .when(col('o_UPDATE_VALIDATOR') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_SDS_CUSTOMER_PET_2, type TARGET 
# COLUMN COUNT: 13


Shortcut_to_SDS_CUSTOMER_PET_2 = UPD_SDS_CUSTOMER_PET.selectExpr(
	"CAST(ID AS STRING) as SDS_CUSTOMER_PET_ID",
	"CAST(NAME AS STRING) as SDS_CUSTOMER_PET_NAME",
	"CAST(PSVC_CUSTOMER_C AS STRING) as SDS_ACCOUNT_ID",
	"CAST(PSVC_PET_C AS STRING) as SDS_ASSET_ID",
	"CAST(CURRENCY_ISO_CODE AS STRING) as CURRENCY_ISO_CD",
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
	"CAST(SYSTEM_MOD_STAMP AS TIMESTAMP) as SDS_SYSTEM_MODIFIED_TSTMP",
	"CAST(LAST_MODIFIED_DATE AS TIMESTAMP) as SDS_LAST_MODIFIED_TSTMP",
	"CAST(LAST_MODIFIED_BY_ID AS STRING) as SDS_LAST_MODIFIED_BY_ID",
	"CAST(CREATED_DATE AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(CREATED_BY_ID AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_SDS_CUSTOMER_PET.pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.SDS_CUSTOMER_PET_ID = target.SDS_CUSTOMER_PET_ID"""
  refined_perf_table = f"{legacy}.SDS_CUSTOMER_PET"
  executeMerge(Shortcut_to_SDS_CUSTOMER_PET_2, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SDS_CUSTOMER_PET", "SDS_CUSTOMER_PET", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SDS_CUSTOMER_PET", "SDS_CUSTOMER_PET","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	