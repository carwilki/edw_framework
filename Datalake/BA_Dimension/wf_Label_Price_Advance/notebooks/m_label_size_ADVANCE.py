# Databricks notebook source
#Code converted on 2023-10-24 21:17:04
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

# Processing node SQ_Shortcut_to_LABEL_SIZE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_LABEL_SIZE = spark.sql(f"""SELECT
LABEL_SIZE_ID
FROM {legacy}.LABEL_SIZE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_LABEL_DAY_STORE_SKU, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_LABEL_DAY_STORE_SKU = spark.sql(f"""SELECT DISTINCT
LABEL_SIZE_ID
FROM {legacy}.LABEL_DAY_STORE_SKU
WHERE UPDATE_TSTMP > CURRENT_DATE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_Label_Size, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp = SQ_Shortcut_to_LABEL_DAY_STORE_SKU.toDF(*["SQ_Shortcut_to_LABEL_DAY_STORE_SKU___" + col for col in SQ_Shortcut_to_LABEL_DAY_STORE_SKU.columns])
SQ_Shortcut_to_LABEL_SIZE_temp = SQ_Shortcut_to_LABEL_SIZE.toDF(*["SQ_Shortcut_to_LABEL_SIZE___" + col for col in SQ_Shortcut_to_LABEL_SIZE.columns])

JNR_Label_Size = SQ_Shortcut_to_LABEL_SIZE_temp.join(SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp,[SQ_Shortcut_to_LABEL_SIZE_temp.SQ_Shortcut_to_LABEL_SIZE___LABEL_SIZE_ID == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_SIZE_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_SIZE_ID as LABEL_SIZE_ID",
	"SQ_Shortcut_to_LABEL_SIZE___LABEL_SIZE_ID as LABEL_SIZE_ID1")

# COMMAND ----------

# Processing node FIL_Existing_Records, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_Label_Size_temp = JNR_Label_Size.toDF(*["JNR_Label_Size___" + col for col in JNR_Label_Size.columns])

FIL_Existing_Records = JNR_Label_Size_temp.selectExpr(
	"JNR_Label_Size___LABEL_SIZE_ID as LABEL_SIZE_ID",
	"JNR_Label_Size___LABEL_SIZE_ID1 as LABEL_SIZE_ID1").filter("LABEL_SIZE_ID1 IS NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Add_Description_and_Load_Date, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_Existing_Records_temp = FIL_Existing_Records.toDF(*["FIL_Existing_Records___" + col for col in FIL_Existing_Records.columns])

EXP_Add_Description_and_Load_Date = FIL_Existing_Records_temp.selectExpr(
	"FIL_Existing_Records___sys_row_id as sys_row_id",
	"FIL_Existing_Records___LABEL_SIZE_ID as LABEL_SIZE_ID",
	"concat('Unavailable_' , FIL_Existing_Records___LABEL_SIZE_ID ) as LABEL_SIZE_DESC",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_LABEL_SIZE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_LABEL_SIZE = EXP_Add_Description_and_Load_Date.selectExpr(
	"CAST(LABEL_SIZE_ID AS STRING) as LABEL_SIZE_ID",
	"CAST(LABEL_SIZE_DESC AS STRING) as LABEL_SIZE_DESC",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

# DuplicateChecker.check_for_duplicate_primary_keys(spark, f'{legacy}.LABEL_SIZE', Shortcut_to_LABEL_SIZE, ['LABEL_SIZE_ID'])
DuplicateChecker.check_for_duplicate_primary_keys(Shortcut_to_LABEL_SIZE, ['LABEL_SIZE_ID'])


Shortcut_to_LABEL_SIZE.write.mode('append').saveAsTable(f'{legacy}.LABEL_SIZE')
