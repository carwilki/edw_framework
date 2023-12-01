# Databricks notebook source
# Code converted on 2023-10-24 09:48:30
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")


if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node SQ_Shortcut_To_DAYS, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_To_DAYS = spark.sql(f"""SELECT 

FISCAL_DAY_OF_MO_NBR+23 AS DAY_TYPE_ID, 

DATE_FORMAT(DAY_DT,'MM/dd') AS DATE_TYPE_DESC2, 

DAY_OF_WK_NAME_ABBR AS DATE_TYPE_DESC3,

CASE WHEN WEEK_DT = (SELECT WEEK_DT FROM {enterprise}.DAYS WHERE DAY_DT = CURRENT_DATE -1) THEN 1

	 WHEN WEEK_DT = (SELECT LWK_WEEK_DT FROM {enterprise}.DAYS WHERE DAY_DT = CURRENT_DATE -1) THEN 1

ELSE 0

END TW_LW_FLAG

FROM {enterprise}.DAYS 

WHERE 

FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM {enterprise}.DAYS WHERE DAY_DT < CURRENT_DATE)""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_DAYS = SQ_Shortcut_To_DAYS \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[0],'DATE_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[1],'DATE_TYPE_DESC2') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[2],'DATE_TYPE_DESC3') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[3],'TW_LW_FLAG')

# COMMAND ----------

# Processing node EXP_DAYS, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_DAYS_temp = SQ_Shortcut_To_DAYS.toDF(*["SQ_Shortcut_To_DAYS___" + col for col in SQ_Shortcut_To_DAYS.columns])

EXP_DAYS = SQ_Shortcut_To_DAYS_temp.selectExpr(
	"SQ_Shortcut_To_DAYS___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_ID as DATE_TYPE_ID",
	"CASE WHEN LENGTH(TRIM(SQ_Shortcut_To_DAYS___DATE_TYPE_DESC2)) =0 THEN NULL ELSE SQ_Shortcut_To_DAYS___DATE_TYPE_DESC2 END as DATE_TYPE_DESC2",
	"CASE WHEN LENGTH(TRIM(SQ_Shortcut_To_DAYS___DATE_TYPE_DESC3)) = 0 THEN NULL ELSE SQ_Shortcut_To_DAYS___DATE_TYPE_DESC3 END as DATE_TYPE_DESC3",
	"'Active' as DATE_TYPE_5WK_STATUS",
	"SQ_Shortcut_To_DAYS___TW_LW_FLAG as TW_LW_FLAG"
)

# COMMAND ----------

# Processing node SQ_Shortcut_To_WEEKS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_WEEKS = spark.sql(f"""SELECT 

FISCAL_WK_NBR AS DATE_TYPE_DESC2,

CASE WHEN WEEK_DT = (SELECT WEEK_DT FROM {enterprise}.DAYS WHERE DAY_DT = CURRENT_DATE - 1) THEN '1'

	 WHEN WEEK_DT = (SELECT LWK_WEEK_DT FROM {enterprise}.DAYS WHERE DAY_DT = CURRENT_DATE - 1) THEN '1'

ELSE '0'

END TW_LW_FLAG

FROM {enterprise}.Weeks

WHERE 

FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM {enterprise}.DAYS WHERE DAY_DT < CURRENT_DATE)

ORDER BY FISCAL_WK_NBR""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_WEEKS = SQ_Shortcut_To_WEEKS \
	.withColumnRenamed(SQ_Shortcut_To_WEEKS.columns[0],'DATE_TYPE_DESC2') \
	.withColumnRenamed(SQ_Shortcut_To_WEEKS.columns[1],'TW_LW_FLAG')

# COMMAND ----------

# Processing node UPD_DATE_TYPE_ID, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_DAYS_temp = EXP_DAYS.toDF(*["EXP_DAYS___" + col for col in EXP_DAYS.columns])

UPD_DATE_TYPE_ID = EXP_DAYS_temp.selectExpr(
	"EXP_DAYS___DATE_TYPE_ID as DATE_TYPE_ID",
	"EXP_DAYS___DATE_TYPE_DESC2 as DATE_TYPE_DESC2",
	"EXP_DAYS___DATE_TYPE_DESC3 as DATE_TYPE_DESC3",
	"EXP_DAYS___DATE_TYPE_5WK_STATUS as DATE_TYPE_5WK_STATUS",
	"EXP_DAYS___TW_LW_FLAG as TW_LW_FLAG",
 	"1 as pyspark_data_action") 

# COMMAND ----------

# Processing node EXP_WEEKS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_WEEKS_temp = SQ_Shortcut_To_WEEKS.toDF(*["SQ_Shortcut_To_WEEKS___" + col for col in SQ_Shortcut_To_WEEKS.columns])

EXP_WEEKS = SQ_Shortcut_To_WEEKS_temp.withColumn("DATE_TYPE_ID", monotonically_increasing_id()+59) \
	.selectExpr(
	"DATE_TYPE_ID as DATE_TYPE_ID",
	"concat('FW' , ' ' , SQ_Shortcut_To_WEEKS___DATE_TYPE_DESC2 ) as DATE_TYPE_DESC2",
	"'Active' as DATE_TYPE_5WK_STATUS",
	"cast(SQ_Shortcut_To_WEEKS___TW_LW_FLAG as int) as TW_LW_FLAG"
)

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

# COMMAND ----------

# Processing node Shortcut_to_DATE_TYPE_LKUP_DAYS, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_DATE_TYPE_LKUP_DAYS = UPD_DATE_TYPE_ID.selectExpr(
	"DATE_TYPE_ID as DATE_TYPE_ID",
	"CASE WHEN LENGTH(TRIM(DATE_TYPE_DESC2)) = 0 THEN NULL ELSE CAST(DATE_TYPE_DESC2 AS STRING) END as DATE_TYPE_DESC2",
	"CASE WHEN LENGTH(TRIM(DATE_TYPE_DESC3)) = 0 THEN NULL ELSE CAST(DATE_TYPE_DESC3 AS STRING) END as DATE_TYPE_DESC3",
	"CAST(DATE_TYPE_5WK_STATUS AS STRING) as DATE_TYPE_5WK_STATUS",
	"CAST(TW_LW_FLAG AS SMALLINT) as TW_LW_FLAG",
	"pyspark_data_action as pyspark_data_action"
)

try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.DATE_TYPE_LKUP',Shortcut_to_DATE_TYPE_LKUP_DAYS,["KEY1","KEY1"])
 	execute_update(Shortcut_to_DATE_TYPE_LKUP_DAYS, f'{legacy}.DATE_TYPE_LKUP', "source.DATE_TYPE_ID = target.DATE_TYPE_ID")
except Exception as e:
	raise e

# COMMAND ----------

# Processing node UPD_DATE_TYPE_ID1, type UPDATE_STRATEGY 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_WEEKS_temp = EXP_WEEKS.toDF(*["EXP_WEEKS___" + col for col in EXP_WEEKS.columns])

UPD_DATE_TYPE_ID1 = EXP_WEEKS_temp.selectExpr(
	"EXP_WEEKS___DATE_TYPE_ID as DATE_TYPE_ID",
	"CASE WHEN LENGTH(TRIM(EXP_WEEKS___DATE_TYPE_DESC2)) =0 THEN NULL ELSE EXP_WEEKS___DATE_TYPE_DESC2 END as DATE_TYPE_DESC2",
	"EXP_WEEKS___DATE_TYPE_5WK_STATUS as DATE_TYPE_5WK_STATUS",
	"EXP_WEEKS___TW_LW_FLAG as TW_LW_FLAG",
 	"1 as pyspark_data_action") 

# COMMAND ----------

# Processing node Shortcut_to_DATE_TYPE_LKUP_WEEKS, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_DATE_TYPE_LKUP_WEEKS = UPD_DATE_TYPE_ID1.selectExpr(
	"DATE_TYPE_ID as DATE_TYPE_ID",
	"CASE WHEN LENGTH(TRIM(DATE_TYPE_DESC2)) = 0 THEN NULL ELSE  CAST(DATE_TYPE_DESC2 AS STRING) END as DATE_TYPE_DESC2",
	"CAST(DATE_TYPE_5WK_STATUS AS STRING) as DATE_TYPE_5WK_STATUS",
	"CAST(TW_LW_FLAG AS SMALLINT) as TW_LW_FLAG",
	"pyspark_data_action as pyspark_data_action"
)
try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.DATE_TYPE_LKUP',Shortcut_to_DATE_TYPE_LKUP_WEEKS,["KEY1","KEY1"])
  	execute_update(Shortcut_to_DATE_TYPE_LKUP_WEEKS, f'{legacy}.DATE_TYPE_LKUP', "source.DATE_TYPE_ID = target.DATE_TYPE_ID")
except Exception as e:
	raise e
