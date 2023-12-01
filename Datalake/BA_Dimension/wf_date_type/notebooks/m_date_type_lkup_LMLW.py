# Databricks notebook source
# Code converted on 2023-10-24 09:48:32
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
# COLUMN COUNT: 7

SQ_Shortcut_To_DAYS = spark.sql(f"""SELECT 1 AS DATE_TYPE_ID, 'DAY' AS DATE_TYPE_DESC, 49 AS DATE_TYPE_SORT_ID, 'DAY' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 2 AS DATE_TYPE_ID, 'WTD' AS DATE_TYPE_DESC, 50 AS DATE_TYPE_SORT_ID, 'WTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 3 AS DATE_TYPE_ID, 'LW' AS DATE_TYPE_DESC, 51 AS DATE_TYPE_SORT_ID, 'LW' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 4 AS DATE_TYPE_ID, 'PTD' AS DATE_TYPE_DESC, 52 AS DATE_TYPE_SORT_ID, 'PTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 5 AS DATE_TYPE_ID, 'LMth' AS DATE_TYPE_DESC, 53 AS DATE_TYPE_SORT_ID, 'LMth' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 6 AS DATE_TYPE_ID, 'QTD' AS DATE_TYPE_DESC, 54 AS DATE_TYPE_SORT_ID, 'QTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 7 AS DATE_TYPE_ID, 'LQtr' AS DATE_TYPE_DESC, 71 AS DATE_TYPE_SORT_ID, 'LQtr' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 8 AS DATE_TYPE_ID, 'YTD' AS DATE_TYPE_DESC, 56 AS DATE_TYPE_SORT_ID, 'YTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 9 AS DATE_TYPE_ID, '4Wk' AS DATE_TYPE_DESC, 57 AS DATE_TYPE_SORT_ID, '4Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 10 AS DATE_TYPE_ID, '8Wk' AS DATE_TYPE_DESC, 58 AS DATE_TYPE_SORT_ID, '8Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 11 AS DATE_TYPE_ID, '13Wk' AS DATE_TYPE_DESC, 59 AS DATE_TYPE_SORT_ID, '13Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 12 AS DATE_TYPE_ID, 'LYTD' AS DATE_TYPE_DESC, 60 AS DATE_TYPE_SORT_ID, 'LYTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 13 AS DATE_TYPE_ID, 'LYr' AS DATE_TYPE_DESC, 61 AS DATE_TYPE_SORT_ID, 'LYr' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 14 AS DATE_TYPE_ID, '16Wk' AS DATE_TYPE_DESC, 62 AS DATE_TYPE_SORT_ID, '16Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 15 AS DATE_TYPE_ID, 'LYQTD' AS DATE_TYPE_DESC, 63 AS DATE_TYPE_SORT_ID, 'LYQTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 16 AS DATE_TYPE_ID, '2WksAgo' AS DATE_TYPE_DESC, 64 AS DATE_TYPE_SORT_ID, '2WksAgo' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 17 AS DATE_TYPE_ID, '26Wk' AS DATE_TYPE_DESC, 65 AS DATE_TYPE_SORT_ID, '26Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 18 AS DATE_TYPE_ID, '52Wk' AS DATE_TYPE_DESC, 66 AS DATE_TYPE_SORT_ID, '52Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 19 AS DATE_TYPE_ID, 'LYPTD' AS DATE_TYPE_DESC, 67 AS DATE_TYPE_SORT_ID, 'LYPTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 20 AS DATE_TYPE_ID, 'LYLMth' AS DATE_TYPE_DESC, 68 AS DATE_TYPE_SORT_ID, 'LYLMth' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 21 AS DATE_TYPE_ID, '2Wk' AS DATE_TYPE_DESC, 69 AS DATE_TYPE_SORT_ID, '2Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 22 AS DATE_TYPE_ID, 'Q3Q4TD' AS DATE_TYPE_DESC, 70 AS DATE_TYPE_SORT_ID, 'Q3Q4TD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 23 AS DATE_TYPE_ID, 'STD' AS DATE_TYPE_DESC, 55 AS DATE_TYPE_SORT_ID, 'STD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG

UNION

SELECT 24 AS DATE_TYPE_ID, 'Day 1' AS DATE_TYPE_DESC, 9 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 25 AS DATE_TYPE_ID, 'Day 2' AS DATE_TYPE_DESC, 10 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 26 AS DATE_TYPE_ID, 'Day 3' AS DATE_TYPE_DESC, 11 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 27 AS DATE_TYPE_ID, 'Day 4' AS DATE_TYPE_DESC, 12 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 28 AS DATE_TYPE_ID, 'Day 5' AS DATE_TYPE_DESC, 13 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 29 AS DATE_TYPE_ID, 'Day 6' AS DATE_TYPE_DESC, 14 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 30 AS DATE_TYPE_ID, 'Day 7' AS DATE_TYPE_DESC, 15 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 31 AS DATE_TYPE_ID, 'Day 8' AS DATE_TYPE_DESC, 17 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 32 AS DATE_TYPE_ID, 'Day 9' AS DATE_TYPE_DESC, 18 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 33 AS DATE_TYPE_ID, 'Day 10' AS DATE_TYPE_DESC, 19 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 34 AS DATE_TYPE_ID, 'Day 11' AS DATE_TYPE_DESC, 20 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 35 AS DATE_TYPE_ID, 'Day 12' AS DATE_TYPE_DESC, 21 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 36 AS DATE_TYPE_ID, 'Day 13' AS DATE_TYPE_DESC, 22 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 37 AS DATE_TYPE_ID, 'Day 14' AS DATE_TYPE_DESC, 23 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 38 AS DATE_TYPE_ID, 'Day 15' AS DATE_TYPE_DESC, 25 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 39 AS DATE_TYPE_ID, 'Day 16' AS DATE_TYPE_DESC, 26 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 40 AS DATE_TYPE_ID, 'Day 17' AS DATE_TYPE_DESC, 27 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 41 AS DATE_TYPE_ID, 'Day 18' AS DATE_TYPE_DESC, 28 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 42 AS DATE_TYPE_ID, 'Day 19' AS DATE_TYPE_DESC, 29 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 43 AS DATE_TYPE_ID, 'Day 20' AS DATE_TYPE_DESC, 30 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 44 AS DATE_TYPE_ID, 'Day 21' AS DATE_TYPE_DESC, 31 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 45 AS DATE_TYPE_ID, 'Day 22' AS DATE_TYPE_DESC, 33 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Mon' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 46 AS DATE_TYPE_ID, 'Day 23' AS DATE_TYPE_DESC, 34 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Tue' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 47 AS DATE_TYPE_ID, 'Day 24' AS DATE_TYPE_DESC, 35 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Wed' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 48 AS DATE_TYPE_ID, 'Day 25' AS DATE_TYPE_DESC, 36 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Thu' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 49 AS DATE_TYPE_ID, 'Day 26' AS DATE_TYPE_DESC, 37 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Fri' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 50 AS DATE_TYPE_ID, 'Day 27' AS DATE_TYPE_DESC, 38 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sat' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 51 AS DATE_TYPE_ID, 'Day 28' AS DATE_TYPE_DESC, 39 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, 'Sun' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG 

UNION

SELECT 52 AS DATE_TYPE_ID, 'Day 29' AS DATE_TYPE_DESC, 41 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 53 AS DATE_TYPE_ID, 'Day 30' AS DATE_TYPE_DESC, 42 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 54 AS DATE_TYPE_ID, 'Day 31' AS DATE_TYPE_DESC, 43 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 55 AS DATE_TYPE_ID, 'Day 32' AS DATE_TYPE_DESC, 44 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 56 AS DATE_TYPE_ID, 'Day 33' AS DATE_TYPE_DESC, 45 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 57 AS DATE_TYPE_ID, 'Day 34' AS DATE_TYPE_DESC, 46 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 58 AS DATE_TYPE_ID, 'Day 35' AS DATE_TYPE_DESC, 47 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG

UNION

SELECT 59 AS DATE_TYPE_ID, 'Wk 1' AS DATE_TYPE_DESC, 16 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 60 AS DATE_TYPE_ID, 'Wk 2' AS DATE_TYPE_DESC, 24 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 61 AS DATE_TYPE_ID, 'Wk 3' AS DATE_TYPE_DESC, 32 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 62 AS DATE_TYPE_ID, 'Wk 4' AS DATE_TYPE_DESC, 40 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 63 AS DATE_TYPE_ID, 'Wk 5' AS DATE_TYPE_DESC, 48 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Inactive' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG

UNION

SELECT 64 AS DATE_TYPE_ID, 'Day 1 LW' AS DATE_TYPE_DESC, 1 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 65 AS DATE_TYPE_ID, 'Day 2 LW' AS DATE_TYPE_DESC, 2 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 66 AS DATE_TYPE_ID, 'Day 3 LW' AS DATE_TYPE_DESC, 3 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 67 AS DATE_TYPE_ID, 'Day 4 LW' AS DATE_TYPE_DESC, 4 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 68 AS DATE_TYPE_ID, 'Day 5 LW' AS DATE_TYPE_DESC, 5 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 69 AS DATE_TYPE_ID, 'Day 6 LW' AS DATE_TYPE_DESC, 6 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 70 AS DATE_TYPE_ID, 'Day 7 LW' AS DATE_TYPE_DESC, 7 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 71 AS DATE_TYPE_ID, 'LMLW' AS DATE_TYPE_DESC, 8 AS DATE_TYPE_SORT_ID, '' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG

UNION

SELECT 72 AS DATE_TYPE_ID, 'LYLW' AS DATE_TYPE_DESC, 72 AS DATE_TYPE_SORT_ID, 'LYLW' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG

UNION

SELECT 73 AS DATE_TYPE_ID, 'LYSTD' AS DATE_TYPE_DESC, 73 AS DATE_TYPE_SORT_ID, 'LYSTD' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG

UNION

SELECT 74 AS DATE_TYPE_ID, 'YTD2Wk' AS DATE_TYPE_DESC, 74 AS DATE_TYPE_SORT_ID, 'YTD2Wk' AS DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 80 DATE_TYPE_ID, 'Cal YTD' DATE_TYPE_DESC, 80 DATE_TYPE_SORT_ID, 'CYTD' DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 81 DATE_TYPE_ID, 'Cal QTD' DATE_TYPE_DESC, 81 DATE_TYPE_SORT_ID, 'CQTD' DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG UNION

SELECT 82 DATE_TYPE_ID, 'Cal MTD' DATE_TYPE_DESC, 82 DATE_TYPE_SORT_ID, 'CMTD' DATE_TYPE_DESC2, '' AS DATE_TYPE_DESC3, 'Active' AS DATE_TYPE_5WK_STATUS, 0 AS TW_LW_FLAG""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_DAYS = SQ_Shortcut_To_DAYS \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[0],'DATE_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[1],'DATE_TYPE_DESC') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[2],'DATE_TYPE_SORT_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[3],'DATE_TYPE_DESC2') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[4],'DATE_TYPE_DESC3') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[5],'DATE_TYPE_5WK_STATUS') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[6],'TW_LW_FLAG')

# COMMAND ----------

# Processing node EXP_DATE_TYPE_LKUP, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_DAYS_temp = SQ_Shortcut_To_DAYS.toDF(*["SQ_Shortcut_To_DAYS___" + col for col in SQ_Shortcut_To_DAYS.columns])

EXP_DATE_TYPE_LKUP = SQ_Shortcut_To_DAYS_temp.selectExpr(
	"SQ_Shortcut_To_DAYS___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_ID as DATE_TYPE_ID",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_DESC as DATE_TYPE_DESC",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_SORT_ID as DATE_TYPE_SORT_ID",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_DESC2 as DATE_TYPE_DESC2",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_DESC3 as DATE_TYPE_DESC3",
	"SQ_Shortcut_To_DAYS___DATE_TYPE_5WK_STATUS as DATE_TYPE_5WK_STATUS",
	"SQ_Shortcut_To_DAYS___TW_LW_FLAG as TW_LW_FLAG"
)

# COMMAND ----------

# Processing node Shortcut_to_DATE_TYPE_LKUP, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_DATE_TYPE_LKUP = EXP_DATE_TYPE_LKUP.selectExpr(
	"CAST(DATE_TYPE_ID AS BIGINT) as DATE_TYPE_ID",
	"CAST(DATE_TYPE_DESC AS STRING) as DATE_TYPE_DESC",
	"CAST(DATE_TYPE_SORT_ID AS INT) as DATE_TYPE_SORT_ID",
    "CASE WHEN LENGTH(TRIM(DATE_TYPE_DESC2)) = 0 THEN NULL ELSE CAST(DATE_TYPE_DESC2 AS STRING) END as DATE_TYPE_DESC2",
	"CASE WHEN LENGTH(TRIM(DATE_TYPE_DESC3)) = 0 THEN NULL ELSE CAST(DATE_TYPE_DESC3 AS STRING) END as DATE_TYPE_DESC3",
	#"CAST(DATE_TYPE_DESC2 AS STRING) as DATE_TYPE_DESC2",
	#"CAST(DATE_TYPE_DESC3 AS STRING) as DATE_TYPE_DESC3",
	"CAST(DATE_TYPE_5WK_STATUS AS STRING) as DATE_TYPE_5WK_STATUS",
	"CAST(TW_LW_FLAG AS SMALLINT) as TW_LW_FLAG"
)
try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.DATE_TYPE_LKUP',Shortcut_to_DATE_TYPE_LKUP,["KEY1","KEY1"])
	Shortcut_to_DATE_TYPE_LKUP.write.mode("overwrite").saveAsTable(f'{legacy}.DATE_TYPE_LKUP')
except Exception as e:
	raise e
