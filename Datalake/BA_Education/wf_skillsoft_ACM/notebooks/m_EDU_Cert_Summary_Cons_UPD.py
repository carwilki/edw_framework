# Databricks notebook source
#Code converted on 2023-10-17 15:29:05
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

def execute_update(dataframe, target_table, join_condition):
  # Create a temporary view from the DataFrame
  sourceTempView = "temp_source_" + target_table.split(".")[1]

  dataframe.createOrReplaceTempView(sourceTempView)

  merge_sql =  f"""MERGE INTO {target_table} target
                   USING {sourceTempView} source
                   ON {join_condition}
                   WHEN MATCHED THEN UPDATE
                      SET {", ".join([f"target.{col} = source.{col}" for col in dataframe.columns if col!='pyspark_data_action'])}          
                """

  spark.sql(merge_sql)


def has_duplicates(df: DataFrame, key_columns: list) -> bool:
	"""
	Check if a DataFrame has duplicate rows based on the specified key columns.

	:param df: The DataFrame to check for duplicates.
	:param key_columns: A list of column names to use as the key for checking duplicates.
	:return: True if duplicates are found, False otherwise.
	"""
	if df.count() == 0:
		return False
	# Group by the key columns and count occurrences
	grouped = df.groupBy(key_columns).count()

	# Find the maximum count
	max_count = grouped.agg({"count": "max"}).collect()[0][0]

	# Return True if duplicates found, False otherwise
	return max_count > 1

# COMMAND ----------

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_CERT_SUMMARY_CONS, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_EDU_CERT_SUMMARY_CONS = spark.sql(f"""SELECT
DAY_DT,
EMPLOYEE_ID,
MISSED_ASSESS_MID,
MISSED_ASSESS_LID,
MISSED_ASSESS_NAME,
JOB_CD,
LOCATION_ID,
CURR_COMPLIANCE_FLAG,
LOAD_DT
FROM {legacy}.EDU_CERT_SUMMARY_CONS
WHERE CURR_COMPLIANCE_FLAG <> 3""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS = spark.sql(f"""SELECT
SNAPSHOT_DT,
EMPLOYEE_ID,
LEARNING_ID,
LEARNING_NAME,
EXEMPT_FLAG
FROM {legacy}.EDU_SKILLSOFT_LEARNING_EXEMPTIONS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Concat_IDs, type EXPRESSION 
# COLUMN COUNT: 10

EXP_Concat_IDs = SQ_Shortcut_to_EDU_CERT_SUMMARY_CONS.withColumn("Join_Learning_ID", 
                                                                 expr("CONCAT ( IF (MISSED_ASSESS_MID = - 1, 0, MISSED_ASSESS_MID) , IF (MISSED_ASSESS_LID = - 1, 0, MISSED_ASSESS_LID) )"))


# COMMAND ----------

# Processing node FTR_Snapshot_Dt, type FILTER 
# COLUMN COUNT: 5


FTR_Snapshot_Dt = SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.selectExpr(
	"SNAPSHOT_DT as SNAPSHOT_DT",
	"EMPLOYEE_ID as EMPLOYEE_ID",
	"LEARNING_ID as LEARNING_ID",
	"LEARNING_NAME as LEARNING_NAME",
	"EXEMPT_FLAG as EXEMPT_FLAG").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_FLAT_FILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FTR_Snapshot_Dt_temp = FTR_Snapshot_Dt.toDF(*["FTR_Snapshot_Dt___" + col for col in FTR_Snapshot_Dt.columns])
EXP_Concat_IDs_temp = EXP_Concat_IDs.toDF(*["EXP_Concat_IDs___" + col for col in EXP_Concat_IDs.columns])

JNR_FLAT_FILE = FTR_Snapshot_Dt_temp.join(EXP_Concat_IDs_temp,[FTR_Snapshot_Dt_temp.FTR_Snapshot_Dt___EMPLOYEE_ID == EXP_Concat_IDs_temp.EXP_Concat_IDs___EMPLOYEE_ID, FTR_Snapshot_Dt_temp.FTR_Snapshot_Dt___LEARNING_ID == EXP_Concat_IDs_temp.EXP_Concat_IDs___Join_Learning_ID],'inner').selectExpr(
	"EXP_Concat_IDs___DAY_DT as DAY_DT",
	"EXP_Concat_IDs___EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_Concat_IDs___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"EXP_Concat_IDs___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"EXP_Concat_IDs___Join_Learning_ID as Join_Learning_ID",
	"EXP_Concat_IDs___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"EXP_Concat_IDs___JOB_CD as JOB_CD",
	"EXP_Concat_IDs___LOCATION_ID as LOCATION_ID",
	"EXP_Concat_IDs___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"EXP_Concat_IDs___LOAD_DT as LOAD_DT",
	"FTR_Snapshot_Dt___EMPLOYEE_ID as employee_id1",
	"FTR_Snapshot_Dt___LEARNING_ID as learning_id",
	"FTR_Snapshot_Dt___LEARNING_NAME as learning_name",
	"FTR_Snapshot_Dt___EXEMPT_FLAG as exempt_flag")

# COMMAND ----------

# Processing node EXP_Flag_Updates, type EXPRESSION 
# COLUMN COUNT: 12

EXP_Flag_Updates = JNR_FLAT_FILE.withColumn("EXP_Flags", expr("IF (exempt_flag = 1, 3, CURR_COMPLIANCE_FLAG)"))



# COMMAND ----------

# Processing node UPD_Flag, type UPDATE_STRATEGY 
# COLUMN COUNT: 12


UPD_Flag = EXP_Flag_Updates.withColumn('pyspark_data_action', expr("CASE WHEN (exempt_flag = 1) THEN 1 ELSE NULL END"))

# COMMAND ----------

# Processing node Shortcut_to_EDU_CERT_SUMMARY_CONS1, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_EDU_CERT_SUMMARY_CONS1 = UPD_Flag.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(EMPLOYEE_ID AS BIGINT) as EMPLOYEE_ID",
	"CAST(MISSED_ASSESS_MID AS BIGINT) as MISSED_ASSESS_MID",
	"CAST(MISSED_ASSESS_LID AS BIGINT) as MISSED_ASSESS_LID",
	"CAST(CURR_COMPLIANCE_FLAG AS TINYINT) as CURR_COMPLIANCE_FLAG",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.DAY_DT = target.DAY_DT AND source.EMPLOYEE_ID = target.EMPLOYEE_ID AND source.MISSED_ASSESS_MID = target.MISSED_ASSESS_MID AND source.MISSED_ASSESS_LID = target.MISSED_ASSESS_LID"""
	refined_perf_table = f"{legacy}.EDU_CERT_SUMMARY_CONS"

	if has_duplicates(Shortcut_to_EDU_CERT_SUMMARY_CONS1, ["DAY_DT", "EMPLOYEE_ID", "MISSED_ASSESS_MID", "MISSED_ASSESS_LID"]):
		raise Exception("Duplicates found in the dataset")

	execute_update(Shortcut_to_EDU_CERT_SUMMARY_CONS1, refined_perf_table, primary_key)
	logger.info(f"UPDATE with {refined_perf_table} completed]")
	logPrevRunDt("EDU_CERT_SUMMARY_CONS", "EDU_CERT_SUMMARY_CONS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("EDU_CERT_SUMMARY_CONS", "EDU_CERT_SUMMARY_CONS","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		

# COMMAND ----------


