# Databricks notebook source
#Code converted on 2023-10-18 17:16:42
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script
# Read in relation source variables
(username, password, connection_string) = Get_Smart_Training_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_TRAINING_STATUS, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_GS_TRAINING_STATUS = spark.sql(f"""SELECT
GS_TRAINING_STATUS_ID,
GS_TRAINING_STATUS_NAME,
UPDATE_DT,
LOAD_DT
FROM {legacy}.GS_TRAINING_STATUS""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_TRAINING_STATUS = SQ_Shortcut_to_GS_TRAINING_STATUS \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_STATUS.columns[0],'GS_TRAINING_STATUS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_STATUS.columns[1],'GS_TRAINING_STATUS_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_STATUS.columns[2],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_STATUS.columns[3],'LOAD_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_TrainingStatus, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_GS_TrainingStatus = jdbcSqlServerConnection(f"""(SELECT
StatusID,
StatusName
FROM GetSmartTraining_V1.dbo.GS_TrainingStatus) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_TrainingStatus = SQ_Shortcut_to_GS_TrainingStatus \
	.withColumnRenamed(SQ_Shortcut_to_GS_TrainingStatus.columns[0],'StatusID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TrainingStatus.columns[1],'StatusName')

# COMMAND ----------

# Processing node JNR_TRAINING_STATUS, type JOINER 
# COLUMN COUNT: 6

JNR_TRAINING_STATUS = SQ_Shortcut_to_GS_TrainingStatus.join(SQ_Shortcut_to_GS_TRAINING_STATUS,[SQ_Shortcut_to_GS_TrainingStatus.StatusID == SQ_Shortcut_to_GS_TRAINING_STATUS.GS_TRAINING_STATUS_ID],'fullouter')

# COMMAND ----------

# Processing node EXP_Training_Status, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_STATUS_temp = JNR_TRAINING_STATUS.toDF(*["JNR_TRAINING_STATUS___" + col for col in JNR_TRAINING_STATUS.columns])

EXP_Training_Status = JNR_TRAINING_STATUS_temp.selectExpr(
	"JNR_TRAINING_STATUS___StatusID as StatusID",
	"JNR_TRAINING_STATUS___StatusName as StatusName",
	"JNR_TRAINING_STATUS___GS_TRAINING_STATUS_ID as GS_TRAINING_STATUS_ID",
	"JNR_TRAINING_STATUS___GS_TRAINING_STATUS_NAME as GS_TRAINING_STATUS_NAME",
	"JNR_TRAINING_STATUS___LOAD_DT as i_LOAD_DT").selectExpr(
	# "JNR_TRAINING_STATUS___sys_row_id as sys_row_id",
	"StatusID as StatusID",
	"StatusName as StatusName",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (i_LOAD_DT IS NULL , CURRENT_TIMESTAMP, i_LOAD_DT) as LOAD_DT",
	"IF (GS_TRAINING_STATUS_ID IS NULL, 0, IF (IF (StatusName IS NULL, 'zznullzz', StatusName) <> IF (GS_TRAINING_STATUS_NAME IS NULL, 'zznull', GS_TRAINING_STATUS_NAME), 1, 3)) as UpdateStrategy"
)

# COMMAND ----------

# Processing node FIL_Strategy, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_Training_Status_temp = EXP_Training_Status.toDF(*["EXP_Training_Status___" + col for col in EXP_Training_Status.columns])

FIL_Strategy = EXP_Training_Status_temp.selectExpr(
	"EXP_Training_Status___StatusID as StatusID",
	"EXP_Training_Status___StatusName as StatusName",
	"EXP_Training_Status___UPDATE_DT as UPDATE_DT",
	"EXP_Training_Status___LOAD_DT as LOAD_DT",
	"EXP_Training_Status___UpdateStrategy as UpdateStrategy").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_Strategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FIL_Strategy_temp = FIL_Strategy.toDF(*["FIL_Strategy___" + col for col in FIL_Strategy.columns])

UPD_Strategy = FIL_Strategy_temp.selectExpr(
	"FIL_Strategy___StatusID as StatusID",
	"FIL_Strategy___StatusName as StatusName",
	"FIL_Strategy___UPDATE_DT as UPDATE_DT",
	"FIL_Strategy___LOAD_DT as LOAD_DT",
	"FIL_Strategy___UpdateStrategy as UpdateStrategy") \
	.withColumn('pyspark_data_action', col('UpdateStrategy'))

# COMMAND ----------

# Processing node Shortcut_to_GS_TRAINING_STATUS1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_GS_TRAINING_STATUS1 = UPD_Strategy.selectExpr(
	"CAST(StatusID AS INT) as GS_TRAINING_STATUS_ID",
	"CAST(StatusName AS STRING) as GS_TRAINING_STATUS_NAME",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.GS_TRAINING_STATUS_ID = target.GS_TRAINING_STATUS_ID"""
  refined_perf_table = f"{legacy}.GS_TRAINING_STATUS"
  chk=DuplicateChecker()
  chk.check_for_duplicate_primary_keys(Shortcut_to_GS_TRAINING_STATUS1,["GS_TRAINING_STATUS_ID"])
  executeMerge(Shortcut_to_GS_TRAINING_STATUS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed")
  logPrevRunDt("GS_TRAINING_STATUS", "GS_TRAINING_STATUS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GS_TRAINING_STATUS", "GS_TRAINING_STATUS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


