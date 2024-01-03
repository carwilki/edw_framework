# Databricks notebook source
# Code converted on 2023-08-24 09:26:38
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE = spark.sql(f"""SELECT LOCATION_ID, WEEK_DT, SLOT_ITEM_SCORE_ID,

SLOT_ID, SLOTITEM_ID, PRODUCT_ID, CATEGORY, CAT_NAME,

CNSTR_GRP_NAME, CNSTR_NAME, CNSTR_TYPE, SCORE,

IMPORTANCE, LEGAL_FIT, LEGAL_FIT_REASON, CURRENT_DATE AS LOAD_DT

FROM {raw}.SLOT_VIOLATION_DETAIL_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE = SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[1],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[2],'SLOT_ITEM_SCORE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[3],'SLOT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[4],'SLOTITEM_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[5],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[6],'CATEGORY') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[7],'CAT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[8],'CNSTR_GRP_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[9],'CNSTR_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[10],'CNSTR_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[11],'SCORE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[12],'IMPORTANCE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[13],'LEGAL_FIT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[14],'LEGAL_FIT_REASON') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[15],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_SLOT_VIOLATION_DETAIL1, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_SLOT_VIOLATION_DETAIL1 = SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.selectExpr(
	"CAST(LOCATION_ID AS INT) as DC_LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(SLOT_ITEM_SCORE_ID AS INT) as SL_ITEM_SCORE_ID",
	"CAST(SLOT_ID AS INT) as SL_SLOT_ID",
	"CAST(SLOTITEM_ID AS INT) as SL_SLOTITEM_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(CATEGORY AS STRING) as SL_CATEGORY",
	"CAST(CAT_NAME AS STRING) as SL_CAT_NAME",
	"CAST(CNSTR_GRP_NAME AS STRING) as SL_CNSTR_GRP_NAME",
	"CAST(CNSTR_NAME AS STRING) as SL_CNSTR_NAME",
	"CAST(CNSTR_TYPE AS STRING) as SL_CNSTR_TYPE",
	"CAST(SCORE AS DECIMAL(9,4)) as SL_SCORE",
	"CAST(IMPORTANCE AS DECIMAL(9,2)) as SL_IMPORTANCE",
	"LEGAL_FIT as SL_LEGAL_FIT",
	"CAST(LEGAL_FIT_REASON AS INT) as SL_LEGAL_FIT_REASON",
	"CAST(LOAD_DT AS DATE) as UPDATE_DT",
	"CAST(LOAD_DT AS DATE) as LOAD_DT"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_VIOLATION_DETAIL1,'DC_NBR',dcnbr,f'{raw}.SLOT_VIOLATION_DETAIL')
Shortcut_to_SLOT_VIOLATION_DETAIL1.write.mode("append").saveAsTable(f'{legacy}.SLOT_VIOLATION_DETAIL')
