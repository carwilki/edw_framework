# Databricks notebook source
#Code converted on 2023-09-26 15:05:15
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

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

# COMMAND ----------

# Processing node ASQ_Shortcut_to_REPLENISHMENT_DAY, type SOURCE 
# COLUMN COUNT: 12

ASQ_Shortcut_to_REPLENISHMENT_DAY = spark.sql(f"""SELECT   RP.SKU_NBR,

           RP.STORE_NBR,

           RP.DELETE_IND,

           RP.SAFETY_QTY,

           RP.SERVICE_LVL_RT,

           RP.REORDER_POINT_QTY,

           RP.PLAN_DELIV_DAYS,

           RP.TARGET_STOCK_QTY,

           RP.PRESENT_QTY,

           RP.PROMO_QTY,

          CURRENT_TIMESTAMP,

           RD.SKU_NBR as OLD_SKU_NBR

           FROM   {raw}.REPLENISHMENT_PRE RP LEFT OUTER JOIN   {legacy}.REPLENISHMENT_DAY RD   ON  RP.SKU_NBR = RD.SKU_NBR

    AND RP.STORE_NBR = RD.STORE_NBR""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_to_REPLENISHMENT_DAY = ASQ_Shortcut_to_REPLENISHMENT_DAY \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[0],'SKU_NBR') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[1],'STORE_NBR') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[2],'DELETE_IND') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[3],'SAFETY_QTY') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[4],'SERVICE_LVL_RT') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[5],'REORDER_POINT_QTY') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[6],'PLAN_DELIV_DAYS') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[7],'TARGET_STOCK_QTY') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[8],'PRESENT_QTY') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[9],'PROMO_QTY') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[10],'LOAD_DT') \
	.withColumnRenamed(ASQ_Shortcut_to_REPLENISHMENT_DAY.columns[11],'OLD_SKU_NBR')

# COMMAND ----------

# Processing node UPD_REPLENISHMENT_DAY, type UPDATE_STRATEGY 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
# ASQ_Shortcut_to_REPLENISHMENT_DAY_temp = ASQ_Shortcut_to_REPLENISHMENT_DAY.toDF(*["ASQ_Shortcut_to_REPLENISHMENT_DAY___" + col for col in ASQ_Shortcut_to_REPLENISHMENT_DAY.columns])

UPD_REPLENISHMENT_DAY = ASQ_Shortcut_to_REPLENISHMENT_DAY.selectExpr(
	"SKU_NBR as SKU_NBR",
	"STORE_NBR as STORE_NBR",
	"DELETE_IND as DELETE_IND",
	"SAFETY_QTY as SAFETY_QTY",
	"SERVICE_LVL_RT as SERVICE_LVL_RT",
	"REORDER_POINT_QTY as REORDER_POINT_QTY",
	"PLAN_DELIV_DAYS as PLAN_DELIV_DAYS",
	"TARGET_STOCK_QTY as TARGET_STOCK_QTY",
	"PRESENT_QTY as PRESENT_QTY",
	"PROMO_QTY as PROMO_QTY",
	"LOAD_DT as LOAD_DT",
	"OLD_SKU_NBR as OLD_SKU_NBR") \
	.withColumn('pyspark_data_action', when((col('OLD_SKU_NBR').isNull()) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_To_REPLENISHMENT_DAY_1, type TARGET 
# COLUMN COUNT: 11


Shortcut_To_REPLENISHMENT_DAY_1 = UPD_REPLENISHMENT_DAY.selectExpr(
	"CAST(SKU_NBR as int) as SKU_NBR",
	"CAST(STORE_NBR as int) as STORE_NBR",
	"CAST(DELETE_IND AS STRING) as DELETE_IND",
	"CAST(SAFETY_QTY as int) as SAFETY_QTY",
	"CAST(SERVICE_LVL_RT AS DECIMAL(3,1)) as SERVICE_LVL_RT",
	"CAST(REORDER_POINT_QTY as int) as REORDER_POINT_QTY",
	"CAST(PLAN_DELIV_DAYS as smallint) as PLAN_DELIV_DAYS",
	"CAST(TARGET_STOCK_QTY as int) as TARGET_STOCK_QTY",
	"CAST(PRESENT_QTY as int) as PRESENT_QTY",
	"CAST(PROMO_QTY as int) as PROMO_QTY",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)
# Shortcut_To_REPLENISHMENT_DAY_1.write.saveAsTable(f'{refine}.REPLENISHMENT_DAY', mode = 'overwrite')
try:
  primary_key = """source.SKU_NBR = target.SKU_NBR AND source.STORE_NBR = target.STORE_NBR"""
  refined_perf_table = f"{legacy}.REPLENISHMENT_DAY"
  executeMerge(Shortcut_To_REPLENISHMENT_DAY_1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("REPLENISHMENT_DAY", "REPLENISHMENT_DAY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("REPLENISHMENT_DAY", "REPLENISHMENT_DAY","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


