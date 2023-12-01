# Databricks notebook source
# Code converted on 2023-10-24 09:45:10
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

# Processing node LKP_WEEKS_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_WEEKS_SRC = spark.sql(f"""SELECT
WEEK_DT,
FISCAL_WK,
FISCAL_WK_NBR,
FISCAL_YR
FROM {enterprise}.WEEKS""")
# Conforming fields names to the component layout
LKP_WEEKS_SRC = LKP_WEEKS_SRC \
	.withColumnRenamed(LKP_WEEKS_SRC.columns[0],'WEEK_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_To_DAYS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_DAYS = spark.sql(f"""SELECT
DAYS.WEEK_DT,
DAYS.FISCAL_YR
FROM {enterprise}.DAYS
WHERE DAYS.DAY_DT = CURRENT_DATE""").withColumn("sys_row_id", monotonically_increasing_id())

# SQ_Shortcut_To_DAYS = spark.sql(f"""SELECT
# DAYS.WEEK_DT,
# DAYS.FISCAL_YR
# FROM {enterprise}.DAYS
# WHERE DAYS.DAY_DT = '2023-11-09' """).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Get_LYR_FISCAL_YR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_DAYS_temp = SQ_Shortcut_To_DAYS.toDF(*["SQ_Shortcut_To_DAYS___" + col for col in SQ_Shortcut_To_DAYS.columns])

EXP_Get_LYR_FISCAL_YR = SQ_Shortcut_To_DAYS_temp.selectExpr(
	"SQ_Shortcut_To_DAYS___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_DAYS___FISCAL_YR - 1 as LYR_FISCAL_YR"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_WEEKS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WEEKS = spark.sql(f"""SELECT
WEEKS.FISCAL_YR,
WEEKS.FISCAL_WK_NBR
FROM {enterprise}.WEEKS
ORDER BY 1,2""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node AGG_fiscal_yr, type AGGREGATOR 
# COLUMN COUNT: 2

AGG_fiscal_yr = SQ_Shortcut_to_WEEKS \
	.groupBy(col("FISCAL_YR"))\
	.agg( max(col('FISCAL_WK_NBR')).alias("MAX_FISCAL_WK_NBR")) \
	.withColumn("sys_row_id", monotonically_increasing_id())
 

# COMMAND ----------

# Processing node LKP_WEEKS, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 25

# Joining dataframes SQ_Shortcut_To_DAYS, EXP_Get_LYR_FISCAL_YR to form LKP_WEEKS
LKP_WEEKS_joined = SQ_Shortcut_To_DAYS.join(EXP_Get_LYR_FISCAL_YR, SQ_Shortcut_To_DAYS.sys_row_id == EXP_Get_LYR_FISCAL_YR.sys_row_id, 'inner')

LKP_WEEKS_SRC_temp = LKP_WEEKS_SRC.toDF(*["LKP_WEEKS_SRC___" + col for col in LKP_WEEKS_SRC.columns])


LKP_WEEKS_lookup_result = LKP_WEEKS_joined.join(LKP_WEEKS_SRC_temp, (col('LKP_WEEKS_SRC___WEEK_DT') <= col('WEEK_DT')) & (col('LKP_WEEKS_SRC___FISCAL_YR') >= col('LYR_FISCAL_YR')), 'left') 

LKP_WEEKS = LKP_WEEKS_lookup_result.selectExpr(
	# LKP_WEEKS_lookup_result.sys_row_id,
	"LKP_WEEKS_SRC___WEEK_DT as WEEK_DT",
	"LKP_WEEKS_SRC___FISCAL_WK as FISCAL_WK",
	"LKP_WEEKS_SRC___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"LKP_WEEKS_SRC___FISCAL_YR as FISCAL_YR")


# COMMAND ----------

# Processing node FIL_OLDER_THAN_2012, type FILTER 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
LKP_WEEKS_temp = LKP_WEEKS.toDF(*["LKP_WEEKS___" + col for col in LKP_WEEKS.columns])

FIL_OLDER_THAN_2012 = LKP_WEEKS_temp.selectExpr(
	"LKP_WEEKS___WEEK_DT as WEEK_DT",
	"LKP_WEEKS___FISCAL_WK as FISCAL_WK",
	"LKP_WEEKS___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"LKP_WEEKS___FISCAL_YR as FISCAL_YR").filter("FISCAL_YR >= 2012").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_Normal_Join, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
AGG_fiscal_yr_temp = AGG_fiscal_yr.toDF(*["AGG_fiscal_yr___" + col for col in AGG_fiscal_yr.columns])
FIL_OLDER_THAN_2012_temp = FIL_OLDER_THAN_2012.toDF(*["FIL_OLDER_THAN_2012___" + col for col in FIL_OLDER_THAN_2012.columns])

JNR_Normal_Join = AGG_fiscal_yr_temp.join(FIL_OLDER_THAN_2012_temp,[AGG_fiscal_yr_temp.AGG_fiscal_yr___FISCAL_YR == FIL_OLDER_THAN_2012_temp.FIL_OLDER_THAN_2012___FISCAL_YR],'inner').selectExpr(
	"FIL_OLDER_THAN_2012___WEEK_DT as WEEK_DT",
	"FIL_OLDER_THAN_2012___FISCAL_WK as FISCAL_WK",
	"FIL_OLDER_THAN_2012___FISCAL_WK_NBR as FISCAL_WK_NBR",
	"FIL_OLDER_THAN_2012___FISCAL_YR as FISCAL_YR",
	"AGG_fiscal_yr___FISCAL_YR as FISCAL_YR1",
	"AGG_fiscal_yr___MAX_FISCAL_WK_NBR as MAX_FISCAL_WK_NBR")

# COMMAND ----------

# Processing node Shortcut_to_PLAN_ACT_TO_DATE_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_PLAN_ACT_TO_DATE_PRE = JNR_Normal_Join.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(FISCAL_WK AS INT) as FISCAL_WK",
	"CAST(FISCAL_WK_NBR AS TINYINT) as FISCAL_WK_NBR",
	"CAST(FISCAL_YR AS SMALLINT) as FISCAL_YR",
	"CAST(MAX_FISCAL_WK_NBR AS TINYINT) as MAX_FISCAL_WK_NBR"
)
try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.PLAN_ACT_TO_DATE_PRE',Shortcut_to_PLAN_ACT_TO_DATE_PRE,["KEY1","KEY1"])
	Shortcut_to_PLAN_ACT_TO_DATE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.PLAN_ACT_TO_DATE_PRE')
except Exception as e:
	raise e
