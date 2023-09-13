# Databricks notebook source
# Code converted on 2023-08-22 11:02:00
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

# Processing node ASQ_Shortcut_To_PHYS_INV_HISTORY, type SOURCE 
# COLUMN COUNT: 7

ASQ_Shortcut_To_PHYS_INV_HISTORY = spark.sql(f""" SELECT location_id,  PHYS_INV_TYPE_ID ,  CURR_PLANNED_CNT_DT,  PREV_PLANNED_CNT_DT,  CURR_ACTUAL_CNT_DT,  PREV_ACTUAL_CNT_DT,  CURRENT_DATE FROM ( SELECT location_id, PHYS_INV_TYPE_ID , CURR_PLANNED_CNT_DT, PREV_PLANNED_CNT_DT, CURR_ACTUAL_CNT_DT, PREV_ACTUAL_CNT_DT, PHYS_INV_DOC_NBR, row_number() over (PARTITION BY location_id,PHYS_INV_TYPE_ID ORDER BY day_dt DESC) rn FROM {legacy}.PHYS_INV_HISTORY ) ALIAS_NZ WHERE rn = 1 
""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_PHYS_INV_HISTORY = ASQ_Shortcut_To_PHYS_INV_HISTORY \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[0],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[1],'PHYS_INV_TYPE_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[2],'CURR_PLANNED_CNT_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[3],'PREV_PLANNED_CNT_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[4],'CURR_ACTUAL_CNT_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[5],'PREV_ACTUAL_CNT_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_PHYS_INV_HISTORY.columns[6],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_To_PHYS_INV_CURRENT, type TARGET 
# COLUMN COUNT: 7


Shortcut_To_PHYS_INV_CURRENT = ASQ_Shortcut_To_PHYS_INV_HISTORY.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"PHYS_INV_TYPE_ID as PHYS_INV_TYPE_ID",
	"CAST(CURR_PLANNED_CNT_DT AS TIMESTAMP) as CURR_PLANNED_CNT_DT",
	"CAST(PREV_PLANNED_CNT_DT AS TIMESTAMP) as PREV_PLANNED_CNT_DT",
	"CAST(CURR_ACTUAL_CNT_DT AS TIMESTAMP) as CURR_ACTUAL_CNT_DT",
	"CAST(PREV_ACTUAL_CNT_DT AS TIMESTAMP) as PREV_ACTUAL_CNT_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
# overwriteDeltaPartition(Shortcut_To_PHYS_INV_CURRENT,'DC_NBR',dcnbr,f'{raw}.PHYS_INV_CURRENT')

# COMMAND ----------

try:	
  Shortcut_To_PHYS_INV_CURRENT.write.mode("append").saveAsTable(f'{legacy}.PHYS_INV_CURRENT')
  logPrevRunDt("PHYS_INV_CURRENT", "PHYS_INV_CURRENT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("PHYS_INV_CURRENT", "PHYS_INV_CURRENT","Failed",str(e), f"{raw}.log_run_details")
  raise e

# COMMAND ----------


