# Databricks notebook source
#Code converted on 2023-10-24 21:17:03
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

# Processing node SQ_Shortcut_to_LABEL_DAY_STORE_SKU, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_LABEL_DAY_STORE_SKU = spark.sql(f"""SELECT DISTINCT
LABEL_TYPE_ID
FROM {legacy}.LABEL_DAY_STORE_SKU
WHERE LOAD_TSTMP > CURRENT_DATE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_LABEL_TYPE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_LABEL_TYPE = spark.sql(f"""SELECT
LABEL_TYPE_ID
FROM {legacy}.LABEL_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_Label_Type, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp = SQ_Shortcut_to_LABEL_DAY_STORE_SKU.toDF(*["SQ_Shortcut_to_LABEL_DAY_STORE_SKU___" + col for col in SQ_Shortcut_to_LABEL_DAY_STORE_SKU.columns])
SQ_Shortcut_to_LABEL_TYPE_temp = SQ_Shortcut_to_LABEL_TYPE.toDF(*["SQ_Shortcut_to_LABEL_TYPE___" + col for col in SQ_Shortcut_to_LABEL_TYPE.columns])

JNR_Label_Type = SQ_Shortcut_to_LABEL_TYPE_temp.join(SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp,[SQ_Shortcut_to_LABEL_TYPE_temp.SQ_Shortcut_to_LABEL_TYPE___LABEL_TYPE_ID == SQ_Shortcut_to_LABEL_DAY_STORE_SKU_temp.SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_TYPE_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_LABEL_TYPE___LABEL_TYPE_ID as LABEL_TYPE_ID",
	"SQ_Shortcut_to_LABEL_DAY_STORE_SKU___LABEL_TYPE_ID as LABEL_TYPE_ID1")

# COMMAND ----------

# Processing node FIL_Label_Type, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_Label_Type_temp = JNR_Label_Type.toDF(*["JNR_Label_Type___" + col for col in JNR_Label_Type.columns])

FIL_Label_Type = JNR_Label_Type_temp.selectExpr(
	"JNR_Label_Type___LABEL_TYPE_ID as LABEL_TYPE_ID",
	"JNR_Label_Type___LABEL_TYPE_ID1 as LABEL_TYPE_ID1").filter("LABEL_TYPE_ID IS NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Label_type, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_Label_Type_temp = FIL_Label_Type.toDF(*["FIL_Label_Type___" + col for col in FIL_Label_Type.columns])

EXP_Label_type = FIL_Label_Type_temp.selectExpr(
    "FIL_Label_Type___sys_row_id as sys_row_id",
	"FIL_Label_Type___LABEL_TYPE_ID1 as LABEL_TYPE_ID").selectExpr(
	"sys_row_id as sys_row_id",
	"LABEL_TYPE_ID as LABEL_TYPE_ID",
	"concat('Unavailable_' , LABEL_TYPE_ID ) as LABEL_TYPE_DESC",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_LABEL_TYPE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_LABEL_TYPE = EXP_Label_type.selectExpr(
	"CAST(LABEL_TYPE_ID AS TINYINT) as  LABEL_TYPE_ID",
	"CAST(LABEL_TYPE_DESC AS STRING) as LABEL_TYPE_DESC",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

# DuplicateChecker.check_for_duplicate_primary_keys(spark, f'{legacy}.LABEL_TYPE', Shortcut_to_LABEL_TYPE, ['LABEL_TYPE_ID'])
DuplicateChecker.check_for_duplicate_primary_keys(Shortcut_to_LABEL_TYPE, ['LABEL_TYPE_ID'])

Shortcut_to_LABEL_TYPE.write.mode('append').saveAsTable(f'{legacy}.LABEL_TYPE')
