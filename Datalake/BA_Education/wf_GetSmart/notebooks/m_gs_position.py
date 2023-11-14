# Databricks notebook source
#Code converted on 2023-10-18 17:16:38
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

# Processing node SQ_Shortcut_to_GS_PositionType, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_GS_PositionType = jdbcSqlServerConnection(f"""(SELECT
PositionID,
Position,
TrainingTypeID,
PositionType
FROM GetSmartTraining_V1.dbo.GS_PositionType) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_PositionType = SQ_Shortcut_to_GS_PositionType \
	.withColumnRenamed(SQ_Shortcut_to_GS_PositionType.columns[0],'PositionID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_PositionType.columns[1],'Position') \
	.withColumnRenamed(SQ_Shortcut_to_GS_PositionType.columns[2],'TrainingTypeID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_PositionType.columns[3],'PositionType')

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_POSITION, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_GS_POSITION = spark.sql(f"""SELECT
GS_POSITION_ID,
GS_POSITION_NAME,
GS_TRAINING_TYPE_ID,
GS_POSITION_TYPE,
UPDATE_DT,
LOAD_DT
FROM {legacy}.GS_POSITION""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_POSITION = SQ_Shortcut_to_GS_POSITION \
	.withColumnRenamed(SQ_Shortcut_to_GS_POSITION.columns[0],'GS_POSITION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_POSITION.columns[1],'GS_POSITION_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_GS_POSITION.columns[2],'GS_TRAINING_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_POSITION.columns[3],'GS_POSITION_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_GS_POSITION.columns[4],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GS_POSITION.columns[5],'LOAD_DT')

# COMMAND ----------

# Processing node JNR_POSITION_ID, type JOINER 
# COLUMN COUNT: 10

JNR_POSITION_ID = SQ_Shortcut_to_GS_PositionType.join(SQ_Shortcut_to_GS_POSITION,[SQ_Shortcut_to_GS_PositionType.PositionID == SQ_Shortcut_to_GS_POSITION.GS_POSITION_ID],'fullouter')

# COMMAND ----------

# Processing node EXP_Position, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_POSITION_ID_temp = JNR_POSITION_ID.toDF(*["JNR_POSITION_ID___" + col for col in JNR_POSITION_ID.columns])

EXP_Position = JNR_POSITION_ID_temp.selectExpr(
	"JNR_POSITION_ID___PositionID as PositionID",
	"JNR_POSITION_ID___Position as Position",
	"JNR_POSITION_ID___TrainingTypeID as TrainingTypeID",
	"JNR_POSITION_ID___PositionType as PositionType",
	"JNR_POSITION_ID___GS_POSITION_ID as GS_POSITION_ID",
	"JNR_POSITION_ID___GS_POSITION_NAME as GS_POSITION_NAME",
	"JNR_POSITION_ID___GS_TRAINING_TYPE_ID as GS_TRAINING_TYPE_ID",
	"JNR_POSITION_ID___GS_POSITION_TYPE as GS_POSITION_TYPE",
	"JNR_POSITION_ID___LOAD_DT as i_LOAD_DT").selectExpr(
	# "JNR_POSITION_ID___sys_row_id as sys_row_id",
	"PositionID as PositionID",
	"Position as Position",
	"TrainingTypeID as TrainingTypeID",
	"PositionType as PositionType",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (i_LOAD_DT IS NULL , CURRENT_TIMESTAMP, i_LOAD_DT) as LOAD_DT",
	"IF (GS_POSITION_ID IS NULL, 0, IF (IF (Position IS NULL, 'zznullzz', Position) <> IF (GS_POSITION_NAME IS NULL, 'zznull', GS_POSITION_NAME) OR IF (TrainingTypeID IS NULL, - 1, TrainingTypeID) <> IF (GS_TRAINING_TYPE_ID IS NULL, - 1, GS_TRAINING_TYPE_ID) OR IF (PositionType IS NULL, 'zznullzz', PositionType) <> IF (GS_POSITION_TYPE IS NULL, 'zznullzz', GS_POSITION_TYPE), 1, 3)) as UpdateStrategy"
)

# COMMAND ----------

# Processing node FIL_Strategy, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_Position_temp = EXP_Position.toDF(*["EXP_Position___" + col for col in EXP_Position.columns])

FIL_Strategy = EXP_Position_temp.selectExpr(
	"EXP_Position___PositionID as PositionID",
	"EXP_Position___Position as Position",
	"EXP_Position___TrainingTypeID as TrainingTypeID",
	"EXP_Position___PositionType as PositionType",
	"EXP_Position___UPDATE_DT as UPDATE_DT",
	"EXP_Position___LOAD_DT as LOAD_DT",
	"EXP_Position___UpdateStrategy as UpdateStrategy").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_Strategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_Strategy_temp = FIL_Strategy.toDF(*["FIL_Strategy___" + col for col in FIL_Strategy.columns])

UPD_Strategy = FIL_Strategy_temp.selectExpr(
	"FIL_Strategy___PositionID as PositionID",
	"FIL_Strategy___Position as Position",
	"FIL_Strategy___TrainingTypeID as TrainingTypeID",
	"FIL_Strategy___PositionType as PositionType",
	"FIL_Strategy___UPDATE_DT as UPDATE_DT",
	"FIL_Strategy___LOAD_DT as LOAD_DT",
	"FIL_Strategy___UpdateStrategy as UpdateStrategy") \
	.withColumn('pyspark_data_action', col('UpdateStrategy'))

# COMMAND ----------

# Processing node Shortcut_to_GS_POSITION1, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_GS_POSITION1 = UPD_Strategy.selectExpr(
	"CAST(PositionID AS INT) as GS_POSITION_ID",
	"CAST(Position AS STRING) as GS_POSITION_NAME",
	"CAST(TrainingTypeID AS INT) as GS_TRAINING_TYPE_ID",
	"CAST(PositionType AS STRING) as GS_POSITION_TYPE",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.GS_POSITION_ID = target.GS_POSITION_ID"""
  refined_perf_table = f"{legacy}.GS_POSITION"
  chk=DuplicateChecker()
  chk.check_for_duplicate_primary_keys(Shortcut_to_GS_POSITION1,["GS_POSITION_ID"])
  executeMerge(Shortcut_to_GS_POSITION1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed")
  logPrevRunDt("GS_POSITION", "GS_POSITION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GS_POSITION", "GS_POSITION","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


