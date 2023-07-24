#Code converted on 2023-07-20 16:58:07
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
# Processing node SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE = spark.sql(f"""SELECT
ID,
IS_DELETED,
ACCOUNT_ID,
CREATED_BY_ID,
CREATED_DATE,
FIELD,
OLD_VALUE,
NEW_VALUE
FROM {raw}.SDS_ACCOUNT_HISTORY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ACCOUNT_HIST, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SDS_ACCOUNT_HIST = spark.sql(f"""SELECT
SDS_ACCOUNT_HIST.SDS_ACCOUNT_HIST_ID,
SDS_ACCOUNT_HIST.SDS_CREATED_TSTMP,
SDS_ACCOUNT_HIST.LOAD_TSTMP
FROM {legacy}.SDS_ACCOUNT_HIST""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_ACCOUNT_HIST, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_ACCOUNT_HIST_temp = SQ_Shortcut_to_SDS_ACCOUNT_HIST.toDF(*["SQ_Shortcut_to_SDS_ACCOUNT_HIST___" + col for col in SQ_Shortcut_to_SDS_ACCOUNT_HIST.columns])
SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE_temp = SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE.toDF(*["SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___" + col for col in SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE.columns])

JNR_SDS_ACCOUNT_HIST = SQ_Shortcut_to_SDS_ACCOUNT_HIST_tempORY_PRE.join(SQ_Shortcut_to_SDS_ACCOUNT_HIST_temp,[SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE_temp.SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___ID == SQ_Shortcut_to_SDS_ACCOUNT_HIST_temp.SQ_Shortcut_to_SDS_ACCOUNT_HIST___SDS_ACCOUNT_HIST_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_SDS_ACCOUNT_HIST___SDS_ACCOUNT_HIST_ID as i_SDS_ACCOUNT_HIST_ID",
	"SQ_Shortcut_to_SDS_ACCOUNT_HIST___SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"SQ_Shortcut_to_SDS_ACCOUNT_HIST___LOAD_TSTMP as i_LOAD_TSTMP",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___ID as ID",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___IS_DELETED as IS_DELETED",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___ACCOUNT_ID as ACCOUNT_ID",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___CREATED_BY_ID as CREATED_BY_ID",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___CREATED_DATE as CREATED_DATE",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___FIELD as FIELD",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___OLD_VALUE as OLD_VALUE",
	"SQ_Shortcut_to_SDS_ACCOUNT_HISTORY_PRE___NEW_VALUE as NEW_VALUE")

# COMMAND ----------
# Processing node FIL_SDS_ACCOUNT_HIST, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_ACCOUNT_HIST_temp = JNR_SDS_ACCOUNT_HIST.toDF(*["JNR_SDS_ACCOUNT_HIST___" + col for col in JNR_SDS_ACCOUNT_HIST.columns])

FIL_SDS_ACCOUNT_HIST = JNR_SDS_ACCOUNT_HIST_temp.selectExpr(
	"JNR_SDS_ACCOUNT_HIST___i_SDS_ACCOUNT_HIST_ID as i_SDS_ACCOUNT_HIST_ID",
	"JNR_SDS_ACCOUNT_HIST___i_SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"JNR_SDS_ACCOUNT_HIST___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"JNR_SDS_ACCOUNT_HIST___ID as ID",
	"JNR_SDS_ACCOUNT_HIST___IS_DELETED as IS_DELETED",
	"JNR_SDS_ACCOUNT_HIST___ACCOUNT_ID as ACCOUNT_ID",
	"JNR_SDS_ACCOUNT_HIST___CREATED_BY_ID as CREATED_BY_ID",
	"JNR_SDS_ACCOUNT_HIST___CREATED_DATE as CREATED_DATE",
	"JNR_SDS_ACCOUNT_HIST___FIELD as FIELD",
	"JNR_SDS_ACCOUNT_HIST___OLD_VALUE as OLD_VALUE",
	"JNR_SDS_ACCOUNT_HIST___NEW_VALUE as NEW_VALUE").filter("i_SDS_ACCOUNT_HIST_ID IS NULL OR ( i_SDS_ACCOUNT_HIST_ID IS NOT NULL AND i_SDS_CREATED_TSTMP != CREATED_DATE )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_ACCOUNT_HIST, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_SDS_ACCOUNT_HIST_temp = FIL_SDS_ACCOUNT_HIST.toDF(*["FIL_SDS_ACCOUNT_HIST___" + col for col in FIL_SDS_ACCOUNT_HIST.columns])

EXP_SDS_ACCOUNT_HIST = FIL_SDS_ACCOUNT_HIST_temp.selectExpr(
	"FIL_SDS_ACCOUNT_HIST___sys_row_id as sys_row_id",
	"FIL_SDS_ACCOUNT_HIST___ID as ID",
	"FIL_SDS_ACCOUNT_HIST___IS_DELETED as IS_DELETED",
	"FIL_SDS_ACCOUNT_HIST___ACCOUNT_ID as ACCOUNT_ID",
	"FIL_SDS_ACCOUNT_HIST___CREATED_BY_ID as CREATED_BY_ID",
	"FIL_SDS_ACCOUNT_HIST___CREATED_DATE as CREATED_DATE",
	"FIL_SDS_ACCOUNT_HIST___FIELD as FIELD",
	"FIL_SDS_ACCOUNT_HIST___OLD_VALUE as OLD_VALUE",
	"FIL_SDS_ACCOUNT_HIST___NEW_VALUE as NEW_VALUE",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_SDS_ACCOUNT_HIST___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_SDS_ACCOUNT_HIST___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_SDS_ACCOUNT_HIST___i_SDS_ACCOUNT_HIST_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------
# Processing node UPD_SDS_ACCOUNT_HIST, type UPDATE_STRATEGY 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_ACCOUNT_HIST_temp = EXP_SDS_ACCOUNT_HIST.toDF(*["EXP_SDS_ACCOUNT_HIST___" + col for col in EXP_SDS_ACCOUNT_HIST.columns])

UPD_SDS_ACCOUNT_HIST = EXP_SDS_ACCOUNT_HIST_temp.selectExpr(
	"EXP_SDS_ACCOUNT_HIST___ID as ID",
	"EXP_SDS_ACCOUNT_HIST___IS_DELETED as IS_DELETED",
	"EXP_SDS_ACCOUNT_HIST___ACCOUNT_ID as ACCOUNT_ID",
	"EXP_SDS_ACCOUNT_HIST___CREATED_BY_ID as CREATED_BY_ID",
	"EXP_SDS_ACCOUNT_HIST___CREATED_DATE as CREATED_DATE",
	"EXP_SDS_ACCOUNT_HIST___FIELD as FIELD",
	"EXP_SDS_ACCOUNT_HIST___OLD_VALUE as OLD_VALUE",
	"EXP_SDS_ACCOUNT_HIST___NEW_VALUE as NEW_VALUE",
	"EXP_SDS_ACCOUNT_HIST___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_ACCOUNT_HIST___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_ACCOUNT_HIST___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR")\
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)) , lit(0)) .when(col('o_UPDATE_VALIDATOR') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_SDS_ACCOUNT_HIST1, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_SDS_ACCOUNT_HIST1 = UPD_SDS_ACCOUNT_HIST.selectExpr(
	"CAST(ID AS STRING) as SDS_ACCOUNT_HIST_ID",
	"CAST(ACCOUNT_ID AS STRING) as SDS_ACCOUNT_ID",
	"CAST(FIELD AS STRING) as SDS_FIELD_NAME",
	"CAST(OLD_VALUE AS STRING) as OLD_VALUE",
	"CAST(NEW_VALUE AS STRING) as NEW_VALUE",
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
	"CAST(CREATED_DATE AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(CREATED_BY_ID AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_SDS_ACCOUNT_HIST.pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.SDS_ACCOUNT_HIST_ID = target.SDS_ACCOUNT_HIST_ID"""
  refined_perf_table = f"{legacy}.SDS_ACCOUNT_HIST"
  executeMerge(Shortcut_to_SDS_ACCOUNT_HIST1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SDS_ACCOUNT_HIST", "SDS_ACCOUNT_HIST", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SDS_ACCOUNT_HIST", "SDS_ACCOUNT_HIST","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	