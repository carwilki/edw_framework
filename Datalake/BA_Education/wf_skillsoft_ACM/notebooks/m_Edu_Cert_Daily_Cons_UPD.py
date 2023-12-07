# Databricks notebook source
#Code converted on 2023-10-17 15:29:04
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

  print(merge_sql)
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

# Processing node SQ_Shortcut_to_EDU_CERT_DAILY_CONS, type SOURCE 
# COLUMN COUNT: 16

_sql = f"""
SELECT
DAY_DT,
EMPLOYEE_ID,
ASSESSMENT_MID,
ASSESSMENT_LID,
TEST_TAKEN_DT,
ASSESSMENT_NAME,
JOB_CD,
LOCATION_ID,
LAST_TEST_SCORE_NBR,
LAST_TEST_PASSED_FLAG,
COMPLIANT_START_DT,
COMPLIANT_EXPIRATION_DT,
CURR_COMPLIANCE_FLAG,
CURR_MISSING_FLAG,
CURR_PERIOD_ATTEMPTS_NBR,
LOAD_DT 
FROM
(SELECT
DAY_DT,
EMPLOYEE_ID,
ASSESSMENT_MID,
ASSESSMENT_LID,
TEST_TAKEN_DT,
ASSESSMENT_NAME,
JOB_CD,
LOCATION_ID,
LAST_TEST_SCORE_NBR,
LAST_TEST_PASSED_FLAG,
COMPLIANT_START_DT,
COMPLIANT_EXPIRATION_DT,
CURR_COMPLIANCE_FLAG,
CURR_MISSING_FLAG,
CURR_PERIOD_ATTEMPTS_NBR,
LOAD_DT,
row_number() over(partition by EMPLOYEE_ID order by EMPLOYEE_ID) as rn
FROM {legacy}.EDU_CERT_DAILY_CONS
WHERE CURR_COMPLIANCE_FLAG <> 3) lkp where lkp.rn =1
"""

SQ_Shortcut_to_EDU_CERT_DAILY_CONS = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Concat_IDS, type EXPRESSION 
# COLUMN COUNT: 17

EXP_Concat_IDS = SQ_Shortcut_to_EDU_CERT_DAILY_CONS.withColumn("JNR_Learning_ID", 
                                                               expr("CONCAT ( IF (ASSESSMENT_MID = - 1, 0, ASSESSMENT_MID) , IF (ASSESSMENT_LID = - 1, 0, ASSESSMENT_LID) )"))


# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS = spark.sql(f"""SELECT
SNAPSHOT_DT,
EMPLOYEE_ID,
LEARNING_ID,
LEARNING_NAME,
EXEMPT_FLAG
FROM {legacy}.EDU_SKILLSOFT_LEARNING_EXEMPTIONS""")

# COMMAND ----------

# Processing node FTR_Snapshot_Dt, type FILTER 
# COLUMN COUNT: 5

FTR_Snapshot_Dt = SQ_Shortcut_to_EDU_SKILLSOFT_LEARNING_EXEMPTIONS.selectExpr(
	"SNAPSHOT_DT as SNAPSHOT_DT",
	"EMPLOYEE_ID as EMPLOYEE_ID",
	"LEARNING_ID as LEARNING_ID",
	"LEARNING_NAME as LEARNING_NAME",
	"EXEMPT_FLAG as EXEMPT_FLAG").filter("2 > 1").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_Flat_File, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
EXP_Concat_IDS_temp = EXP_Concat_IDS.toDF(*["EXP_Concat_IDS___" + col for col in EXP_Concat_IDS.columns])
FTR_Snapshot_Dt_temp = FTR_Snapshot_Dt.toDF(*["FTR_Snapshot_Dt___" + col for col in FTR_Snapshot_Dt.columns])

JNR_Flat_File = FTR_Snapshot_Dt_temp.join(EXP_Concat_IDS_temp,[FTR_Snapshot_Dt_temp.FTR_Snapshot_Dt___EMPLOYEE_ID == EXP_Concat_IDS_temp.EXP_Concat_IDS___EMPLOYEE_ID, FTR_Snapshot_Dt_temp.FTR_Snapshot_Dt___LEARNING_ID == EXP_Concat_IDS_temp.EXP_Concat_IDS___JNR_Learning_ID],'inner').selectExpr(
	"EXP_Concat_IDS___DAY_DT as DAY_DT",
	"EXP_Concat_IDS___EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_Concat_IDS___ASSESSMENT_MID as ASSESSMENT_MID",
	"EXP_Concat_IDS___ASSESSMENT_LID as ASSESSMENT_LID",
	"EXP_Concat_IDS___JNR_Learning_ID as JNR_Learning_ID",
	"EXP_Concat_IDS___TEST_TAKEN_DT as TEST_TAKEN_DT",
	"EXP_Concat_IDS___ASSESSMENT_NAME as ASSESSMENT_NAME",
	"EXP_Concat_IDS___JOB_CD as JOB_CD",
	"EXP_Concat_IDS___LOCATION_ID as LOCATION_ID",
	"EXP_Concat_IDS___LAST_TEST_SCORE_NBR as LAST_TEST_SCORE_NBR",
	"EXP_Concat_IDS___LAST_TEST_PASSED_FLAG as LAST_TEST_PASSED_FLAG",
	"EXP_Concat_IDS___COMPLIANT_START_DT as COMPLIANT_START_DT",
	"EXP_Concat_IDS___COMPLIANT_EXPIRATION_DT as COMPLIANT_EXPIRATION_DT",
	"EXP_Concat_IDS___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"EXP_Concat_IDS___CURR_MISSING_FLAG as CURR_MISSING_FLAG",
	"EXP_Concat_IDS___CURR_PERIOD_ATTEMPTS_NBR as CURR_PERIOD_ATTEMPTS_NBR",
	"EXP_Concat_IDS___LOAD_DT as LOAD_DT",
	"FTR_Snapshot_Dt___EMPLOYEE_ID as EMPLOYEE_ID1",
	"FTR_Snapshot_Dt___LEARNING_ID as learning_id",
	"FTR_Snapshot_Dt___LEARNING_NAME as learning_name",
	"FTR_Snapshot_Dt___EXEMPT_FLAG as exempt_flag")

# COMMAND ----------

# Processing node EXP_Flags, type EXPRESSION 
# COLUMN COUNT: 22

EXP_Flags = JNR_Flat_File.withColumn("EXP_Flags", expr("IF (exempt_flag = 1, 3, CURR_COMPLIANCE_FLAG)"))



# COMMAND ----------

# Processing node UPD_Flag, type UPDATE_STRATEGY 
# COLUMN COUNT: 22


UPD_Flag = EXP_Flags.withColumn('pyspark_data_action', expr("CASE WHEN (exempt_flag = 1) THEN 1 ELSE NULL END"))

# COMMAND ----------

# Processing node Shortcut_to_EDU_CERT_DAILY_CONS1, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_EDU_CERT_DAILY_CONS1 = UPD_Flag.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(EMPLOYEE_ID AS BIGINT) as EMPLOYEE_ID",
	"CAST(ASSESSMENT_MID AS BIGINT) as ASSESSMENT_MID",
	"CAST(ASSESSMENT_LID AS BIGINT) as ASSESSMENT_LID",
	"CAST(CURR_COMPLIANCE_FLAG AS TINYINT) as CURR_COMPLIANCE_FLAG",
	"pyspark_data_action as pyspark_data_action"
)		

# COMMAND ----------

display(Shortcut_to_EDU_CERT_DAILY_CONS1)

# COMMAND ----------

try:
    primary_key = """source.DAY_DT = target.DAY_DT AND source.EMPLOYEE_ID = target.EMPLOYEE_ID AND source.ASSESSMENT_MID = target.ASSESSMENT_MID AND source.ASSESSMENT_LID = target.ASSESSMENT_LID"""
    refined_perf_table = f"{legacy}.EDU_CERT_DAILY_CONS"

    if has_duplicates(Shortcut_to_EDU_CERT_DAILY_CONS1, ["DAY_DT", "EMPLOYEE_ID", "ASSESSMENT_MID", "ASSESSMENT_LID"]):
        raise Exception("Duplicates found in the dataset")

    execute_update(Shortcut_to_EDU_CERT_DAILY_CONS1, refined_perf_table, primary_key)
    logger.info(f"UPDATE with {refined_perf_table} completed]")
    logPrevRunDt("EDU_CERT_DAILY_CONS", "EDU_CERT_DAILY_CONS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("EDU_CERT_DAILY_CONS", "EDU_CERT_DAILY_CONS","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# COMMAND ----------


