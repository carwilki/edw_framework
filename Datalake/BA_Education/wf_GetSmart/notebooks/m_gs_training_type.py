# Databricks notebook source
#Code converted on 2023-10-18 17:16:44
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

# Processing node SQ_Shortcut_to_GS_TrainingType, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_GS_TrainingType = jdbcSqlServerConnection(f"""(SELECT
TypeID,
TrainingName,
CutOffDays,
IsActive
FROM GetSmartTraining_V1.dbo.GS_TrainingType) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_TrainingType = SQ_Shortcut_to_GS_TrainingType \
	.withColumnRenamed(SQ_Shortcut_to_GS_TrainingType.columns[0],'TypeID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TrainingType.columns[1],'TrainingName') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TrainingType.columns[2],'CutOffDays') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TrainingType.columns[3],'IsActive')

# COMMAND ----------

# Processing node SQ_Shortcut_to_GS_TRAINING_TYPE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_GS_TRAINING_TYPE = spark.sql(f"""SELECT
GS_TRAINING_TYPE_ID,
GS_TRAINING_TYPE_NAME,
CUT_OFF_DAYS,
IS_ACTIVE,
LOAD_DT
FROM {legacy}.GS_TRAINING_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_GS_TRAINING_TYPE = SQ_Shortcut_to_GS_TRAINING_TYPE \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_TYPE.columns[0],'GS_TRAINING_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_TYPE.columns[1],'GS_TRAINING_TYPE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_TYPE.columns[2],'CUT_OFF_DAYS') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_TYPE.columns[3],'IS_ACTIVE') \
	.withColumnRenamed(SQ_Shortcut_to_GS_TRAINING_TYPE.columns[4],'LOAD_DT')

# COMMAND ----------

# Processing node JNR_TRAINING_TYPE, type JOINER 
# COLUMN COUNT: 9

JNR_TRAINING_TYPE = SQ_Shortcut_to_GS_TrainingType.join(SQ_Shortcut_to_GS_TRAINING_TYPE,[SQ_Shortcut_to_GS_TrainingType.TypeID == SQ_Shortcut_to_GS_TRAINING_TYPE.GS_TRAINING_TYPE_ID],'fullouter')

# COMMAND ----------

# Processing node EXP_Training_Type, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_TYPE_temp = JNR_TRAINING_TYPE.toDF(*["JNR_TRAINING_TYPE___" + col for col in JNR_TRAINING_TYPE.columns])

EXP_Training_Type = JNR_TRAINING_TYPE_temp.selectExpr(
	"JNR_TRAINING_TYPE___TypeID as TypeID",
	"JNR_TRAINING_TYPE___TrainingName as TrainingName",
	"JNR_TRAINING_TYPE___CutOffDays as CutOffDays",
	"JNR_TRAINING_TYPE___IsActive as i_IsActive",
	"JNR_TRAINING_TYPE___GS_TRAINING_TYPE_ID as GS_TRAINING_TYPE_ID",
	"JNR_TRAINING_TYPE___GS_TRAINING_TYPE_NAME as GS_TRAINING_TYPE_NAME",
	"JNR_TRAINING_TYPE___CUT_OFF_DAYS as CUT_OFF_DAYS",
	"JNR_TRAINING_TYPE___IS_ACTIVE as IS_ACTIVE",
	"JNR_TRAINING_TYPE___LOAD_DT as i_LOAD_DT").selectExpr(
	# "JNR_TRAINING_TYPE___sys_row_id as sys_row_id",
	"TypeID as TypeID",
	"TrainingName as TrainingName",
	"CutOffDays as CutOffDays",
	"IF (i_IsActive = 'T', '1', '0') as IsActive",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (i_LOAD_DT IS NULL , CURRENT_TIMESTAMP, i_LOAD_DT) as LOAD_DT",
	"IF (GS_TRAINING_TYPE_ID IS NULL, 0, IF (IF (TrainingName IS NULL, 'zznullzz', TrainingName) <> IF (GS_TRAINING_TYPE_NAME IS NULL, 'zznull', GS_TRAINING_TYPE_NAME) OR IF (CutOffDays IS NULL, - 1, CutOffDays) <> IF (CUT_OFF_DAYS IS NULL, - 1, CUT_OFF_DAYS) OR IF (IF (i_IsActive = 'T', '1', '0') IS NULL, - 1, IF (i_IsActive = 'T', '1', '0')) <> IF (cast(IS_ACTIVE as int) IS NULL, - 1, cast(IS_ACTIVE as int)), 1, 3)) as UpdateStrategy"
)
# .withColumn("v_IsActive", expr("""IF (JNR_TRAINING_TYPE___i_IsActive = 'T', '1', '0')"""))

# COMMAND ----------

# Processing node FIL_Strategy, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_Training_Type_temp = EXP_Training_Type.toDF(*["EXP_Training_Type___" + col for col in EXP_Training_Type.columns])

FIL_Strategy = EXP_Training_Type_temp.selectExpr(
	"EXP_Training_Type___TypeID as TypeID",
	"EXP_Training_Type___TrainingName as TrainingName",
	"EXP_Training_Type___CutOffDays as CutOffDays",
	"EXP_Training_Type___IsActive as IsActive",
	"EXP_Training_Type___UPDATE_DT as UPDATE_DT",
	"EXP_Training_Type___LOAD_DT as LOAD_DT",
	"EXP_Training_Type___UpdateStrategy as UpdateStrategy").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_Strategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_Strategy_temp = FIL_Strategy.toDF(*["FIL_Strategy___" + col for col in FIL_Strategy.columns])

UPD_Strategy = FIL_Strategy_temp.selectExpr(
	"FIL_Strategy___TypeID as TypeID",
	"FIL_Strategy___TrainingName as TrainingName",
	"FIL_Strategy___CutOffDays as CutOffDays",
	"FIL_Strategy___IsActive as IsActive",
	"FIL_Strategy___UPDATE_DT as UPDATE_DT",
	"FIL_Strategy___LOAD_DT as LOAD_DT",
	"FIL_Strategy___UpdateStrategy as UpdateStrategy") \
	.withColumn('pyspark_data_action', col('UpdateStrategy'))

# COMMAND ----------

# Processing node Shortcut_to_GS_TRAINING_TYPE, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_GS_TRAINING_TYPE = UPD_Strategy.selectExpr(
	"CAST(TypeID AS INT) as GS_TRAINING_TYPE_ID",
	"CAST(TrainingName AS STRING) as GS_TRAINING_TYPE_NAME",
	"CAST(CutOffDays AS INT) as CUT_OFF_DAYS",
	"CAST(IsActive as tinyint) as IS_ACTIVE",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.GS_TRAINING_TYPE_ID = target.GS_TRAINING_TYPE_ID"""
  refined_perf_table = f"{legacy}.GS_TRAINING_TYPE"
  chk=DuplicateChecker()
  chk.check_for_duplicate_primary_keys(Shortcut_to_GS_TRAINING_TYPE,["GS_TRAINING_TYPE_ID"])
  executeMerge(Shortcut_to_GS_TRAINING_TYPE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed")
  logPrevRunDt("GS_TRAINING_TYPE", "GS_TRAINING_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GS_TRAINING_TYPE", "GS_TRAINING_TYPE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


