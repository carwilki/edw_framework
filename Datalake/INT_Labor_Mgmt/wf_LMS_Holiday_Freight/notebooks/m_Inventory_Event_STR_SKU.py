# Databricks notebook source
# Code converted on 2023-09-05 14:08:52
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

# Processing node SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE = spark.sql(f""" SELECT ZTB_EVENT_HOLD_PRE.QUANTITY, SKU_PROFILE.PRODUCT_ID, SITE_PROFILE.LOCATION_ID, STO_TYPE_LU.STO_TYPE_ID 
FROM  {raw}.ZTB_EVENT_HOLD_PRE 
LEFT OUTER JOIN  {legacy}.STO_TYPE_LU  ON  ZTB_EVENT_HOLD_PRE.sto_type = STO_TYPE_LU.sto_type 
LEFT OUTER JOIN  {legacy}.SKU_PROFILE ON  ZTB_EVENT_HOLD_PRE.sku_nbr = SKU_PROFILE.sku_nbr 
LEFT OUTER JOIN  {legacy}.SITE_PROFILE ON  ZTB_EVENT_HOLD_PRE.store_nbr = SITE_PROFILE.store_nbr 
WHERE  STO_TYPE_LU.STR_END_DATE > CURRENT_DATE - 1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE = SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE.columns[0],'QUANTITY') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE.columns[3],'STO_TYPE_ID')

# COMMAND ----------

# Processing node Shortcut_to_INVENTORY_EVENT_STR_SKU1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_INVENTORY_EVENT_STR_SKU1 = SQ_Shortcut_to_ZTB_EVENT_HOLD_PRE.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(STO_TYPE_ID AS INT) as STO_TYPE_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(QUANTITY AS DECIMAL(13,3)) as QUANTITY"
)
Shortcut_to_INVENTORY_EVENT_STR_SKU1.write.mode("overwrite").saveAsTable(f'{legacy}.INVENTORY_EVENT_STR_SKU')

# COMMAND ----------


# try:
#     primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.STO_TYPE_ID = target.STO_TYPE_ID AND source.PRODUCT_ID = target.PRODUCT_ID"""
#     legacy_table = f"{legacy}.INVENTORY_EVENT_STR_SKU"
#     executeMerge(Shortcut_to_INVENTORY_EVENT_STR_SKU1, legacy_table, primary_key)
#     logger.info("Merge with" + legacy_table + "completed")
#     logPrevRunDt("INVENTORY_EVENT_STR_SKU", "INVENTORY_EVENT_STR_SKU", "Completed", "N/A", f"{raw}.log_run_details")
# except Exception as e:
#     logPrevRunDt("INVENTORY_EVENT_STR_SKU", "INVENTORY_EVENT_STR_SKU", "Failed", str(e), f"{raw}.log_run_details")
#     raise e


