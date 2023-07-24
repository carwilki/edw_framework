#Code converted on 2023-07-24 08:23:57
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
# Processing node SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE = spark.sql(f"""SELECT 

SDS_ORDER_HISTORY_PRE.ID,

SDS_ORDER_HISTORY_PRE.IS_DELETED, 

SDS_ORDER_HISTORY_PRE.ORDER_ID, 

SDS_ORDER_HISTORY_PRE.CREATED_BY_ID, 

SDS_ORDER_HISTORY_PRE.CREATED_DATE, 

SDS_ORDER_HISTORY_PRE.FIELD, 

SDS_ORDER_HISTORY_PRE.OLD_VALUE, 

SDS_ORDER_HISTORY_PRE.NEW_VALUE 

FROM {raw}.SDS_ORDER_HISTORY_PRE

JOIN {legacy}.SDS_ORDER

ON SDS_ORDER_HISTORY_PRE.ORDER_ID=SDS_ORDER.SDS_ORDER_ID

WHERE

SDS_ORDER.SMS_ORDER_ID like 'F%' 

OR 

SDS_ORDER.SMS_ORDER_ID IS NULL""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE = SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[0],'ID')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[1],'IS_DELETED')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[2],'ORDER_ID')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[3],'CREATED_BY_ID')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[4],'CREATED_DATE')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[5],'FIELD')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[6],'OLD_VALUE')\
	.withColumnRenamed(SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns[7],'NEW_VALUE')

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_HISTORY, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SDS_ORDER_HISTORY = spark.sql(f"""SELECT
SDS_ORDER_HIST_ID,
SDS_CREATED_TSTMP,
LOAD_TSTMP
FROM {legacy}.SDS_ORDER_HIST""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_ORDER_HISTORY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE_temp = SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.toDF(*["SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___" + col for col in SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE.columns])
SQ_Shortcut_to_SDS_ORDER_HISTORY_temp = SQ_Shortcut_to_SDS_ORDER_HISTORY.toDF(*["SQ_Shortcut_to_SDS_ORDER_HISTORY___" + col for col in SQ_Shortcut_to_SDS_ORDER_HISTORY.columns])

JNR_SDS_ORDER_HISTORY = SQ_Shortcut_to_SDS_ORDER_HISTORY_temp_PRE_temp.join(SQ_Shortcut_to_SDS_ORDER_HISTORY_temp,[SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE_temp.SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___ID == SQ_Shortcut_to_SDS_ORDER_HISTORY_temp.SQ_Shortcut_to_SDS_ORDER_HISTORY___SDS_ORDER_HIST_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_SDS_ORDER_HISTORY___SDS_ORDER_HIST_ID as i_SDS_ORDER_HIST_ID",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY___SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY___LOAD_TSTMP as i_LOAD_TSTMP",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___ID as ID",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___IS_DELETED as IS_DELETED",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___ORDER_ID as ORDER_ID",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___CREATED_BY_ID as CREATED_BY_ID",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___CREATED_DATE as CREATED_DATE",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___FIELD as FIELD",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___OLD_VALUE as OLD_VALUE",
	"SQ_Shortcut_to_SDS_ORDER_HISTORY_PRE___NEW_VALUE as NEW_VALUE")

# COMMAND ----------
# Processing node FIL_SDS_ORDER_HIST, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_ORDER_HISTORY_temp = JNR_SDS_ORDER_HISTORY.toDF(*["JNR_SDS_ORDER_HISTORY___" + col for col in JNR_SDS_ORDER_HISTORY.columns])

FIL_SDS_ORDER_HIST = JNR_SDS_ORDER_HISTORY_temp.selectExpr(
	"JNR_SDS_ORDER_HISTORY___i_SDS_ORDER_HIST_ID as i_SDS_ORDER_HIST_ID",
	"JNR_SDS_ORDER_HISTORY___i_SDS_CREATED_TSTMP as i_SDS_CREATED_TSTMP",
	"JNR_SDS_ORDER_HISTORY___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"JNR_SDS_ORDER_HISTORY___ID as ID",
	"JNR_SDS_ORDER_HISTORY___IS_DELETED as IS_DELETED",
	"JNR_SDS_ORDER_HISTORY___ORDER_ID as ORDER_ID",
	"JNR_SDS_ORDER_HISTORY___CREATED_BY_ID as CREATED_BY_ID",
	"JNR_SDS_ORDER_HISTORY___CREATED_DATE as CREATED_DATE",
	"JNR_SDS_ORDER_HISTORY___FIELD as FIELD",
	"JNR_SDS_ORDER_HISTORY___OLD_VALUE as OLD_VALUE",
	"JNR_SDS_ORDER_HISTORY___NEW_VALUE as NEW_VALUE").filter("i_SDS_ORDER_HIST_ID IS NULL OR ( i_SDS_ORDER_HIST_ID IS NOT NULL AND i_SDS_CREATED_TSTMP != CREATED_DATE )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_ORDER_HISTORY, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
FIL_SDS_ORDER_HIST_temp = FIL_SDS_ORDER_HIST.toDF(*["FIL_SDS_ORDER_HIST___" + col for col in FIL_SDS_ORDER_HIST.columns])

EXP_SDS_ORDER_HISTORY = FIL_SDS_ORDER_HIST_temp.selectExpr(
	"FIL_SDS_ORDER_HIST___sys_row_id as sys_row_id",
	"FIL_SDS_ORDER_HIST___ID as ID",
	"FIL_SDS_ORDER_HIST___IS_DELETED as IS_DELETED",
	"FIL_SDS_ORDER_HIST___ORDER_ID as ORDER_ID",
	"FIL_SDS_ORDER_HIST___CREATED_BY_ID as CREATED_BY_ID",
	"FIL_SDS_ORDER_HIST___CREATED_DATE as CREATED_DATE",
	"FIL_SDS_ORDER_HIST___FIELD as FIELD",
	"FIL_SDS_ORDER_HIST___OLD_VALUE as OLD_VALUE",
	"FIL_SDS_ORDER_HIST___NEW_VALUE as NEW_VALUE",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_SDS_ORDER_HIST___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_SDS_ORDER_HIST___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_SDS_ORDER_HIST___i_SDS_ORDER_HIST_ID IS NULL, 1, 2) as o_UPDATE_TSTMP"
)

# COMMAND ----------
# Processing node UPD_SDS_ORDER_HISTORY, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_ORDER_HISTORY_temp = EXP_SDS_ORDER_HISTORY.toDF(*["EXP_SDS_ORDER_HISTORY___" + col for col in EXP_SDS_ORDER_HISTORY.columns])

UPD_SDS_ORDER_HISTORY = EXP_SDS_ORDER_HISTORY_temp.selectExpr(
	"EXP_SDS_ORDER_HISTORY___ID as ID",
	"EXP_SDS_ORDER_HISTORY___IS_DELETED as IS_DELETED",
	"EXP_SDS_ORDER_HISTORY___ORDER_ID as ORDER_ID",
	"EXP_SDS_ORDER_HISTORY___CREATED_BY_ID as CREATED_BY_ID",
	"EXP_SDS_ORDER_HISTORY___CREATED_DATE as CREATED_DATE",
	"EXP_SDS_ORDER_HISTORY___FIELD as FIELD",
	"EXP_SDS_ORDER_HISTORY___OLD_VALUE as OLD_VALUE",
	"EXP_SDS_ORDER_HISTORY___NEW_VALUE as NEW_VALUE",
	"EXP_SDS_ORDER_HISTORY___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_ORDER_HISTORY___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_ORDER_HISTORY___o_UPDATE_TSTMP as o_UPDATE_VALIDATOR")\
	.withColumn('pyspark_data_action', when(col('o_UPDATE_TSTMP') ==(lit(1)) , lit(0)) .when(col('o_UPDATE_TSTMP') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node SDS_ORDER_HIST, type TARGET 
# COLUMN COUNT: 10


SDS_ORDER_HIST = UPD_SDS_ORDER_HISTORY.selectExpr(
	"CAST(ID AS STRING) as SDS_ORDER_HIST_ID",
	"CAST(ORDER_ID AS STRING) as SDS_ORDER_ID",
	"CAST(FIELD AS STRING) as SDS_FIELD_NAME",
	"CAST(OLD_VALUE AS STRING) as OLD_VALUE",
	"CAST(NEW_VALUE AS STRING) as NEW_VALUE",
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
	"CAST(CREATED_DATE AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(CREATED_BY_ID AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_SDS_ORDER_HISTORY.pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.SDS_ORDER_HIST_ID = target.SDS_ORDER_HIST_ID"""
  refined_perf_table = f"{legacy}.SDS_ORDER_HIST"
  executeMerge(SDS_ORDER_HIST, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SDS_ORDER_HIST", "SDS_ORDER_HIST", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SDS_ORDER_HIST", "SDS_ORDER_HIST","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	