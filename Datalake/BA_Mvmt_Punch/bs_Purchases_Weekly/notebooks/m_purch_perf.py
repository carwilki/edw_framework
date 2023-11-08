# Databricks notebook source
# Code converted on 2023-10-12 09:32:18
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

# Processing node SQ_Shortcut_To_PURCHASES, type SOURCE 
# COLUMN COUNT: 26

SQ_Shortcut_To_PURCHASES = spark.sql(f""" SELECT POT.WEEK_DT, POT.SAP_CATEGORY_ID, POT.VENDOR_ID, POT.PO_TYPE_ID, POT.DISTRICT_ID, POT.STORE_TYPE_ID, POT.RECVD_AFTER_DUE_DAYS_CNT, POT.RECVD_BEFORE_DUE_DAYS_CNT, POT.ACTUAL_LEADTIME_DAYS_CNT, PON.PO_DUE_CNT, PON.PO_RECVD_CNT, PON.PO_LATE_CNT, POT.LINE_ITEM_DUE_CNT, POT.LINE_ITEM_RECVD_CNT, POT.LINE_ITEMS_LATE_CNT, POT.LINE_ITEM_OOB_CNT, POT.DUE_QTY, POT.RECVD_QTY, POT.LATE_QTY, POT.EXCESS_QTY, POT.SHORT_QTY, POT.ORDER_COST_GROSS, POT.ORDER_COST_ACTUAL, POT.ORDER_COST_NET, POT.FREIGHT_COST, CURRENT_TIMESTAMP from {raw}.PURCH_PO_TYPE_PRE POT, ( SELECT PO.WEEK_DT, PO.SAP_CATEGORY_ID,  PO.VENDOR_ID, PO.PO_TYPE_ID, PO.DISTRICT_ID, PO.STORE_TYPE_ID, SUM(PO_DUE_CNT) PO_DUE_CNT, SUM(PO_RECVD_CNT) PO_RECVD_CNT, SUM(PO_LATE_CNT) PO_LATE_CNT from {raw}.PURCH_PO_NBR_PRE PO GROUP BY PO.WEEK_DT, PO.SAP_CATEGORY_ID, PO.VENDOR_ID, PO.PO_TYPE_ID, PO.DISTRICT_ID, PO.STORE_TYPE_ID) PON WHERE POT.WEEK_DT  = PON.WEEK_DT AND POT.SAP_CATEGORY_ID = PON.SAP_CATEGORY_ID AND POT.VENDOR_ID = PON.VENDOR_ID AND POT.PO_TYPE_ID = PON.PO_TYPE_ID AND POT.DISTRICT_ID = PON.DISTRICT_ID AND POT.STORE_TYPE_ID = PON.STORE_TYPE_ID
""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_PURCHASES = SQ_Shortcut_To_PURCHASES \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[0],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[1],'SAP_CATEGORY_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[2],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[3],'PO_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[4],'DISTRICT_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[5],'STORE_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[6],'RECVD_AFTER_DUE_DAYS_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[7],'RECVD_BEFORE_DUE_DAYS_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[8],'ACTUAL_LEADTIME_DAYS_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[9],'PO_DUE_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[10],'PO_RECVD_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[11],'PO_LATE_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[12],'LINE_ITEM_DUE_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[13],'LINE_ITEM_RECVD_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[14],'LINE_ITEMS_LATE_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[15],'LINE_ITEM_OOB_CNT') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[16],'DUE_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[17],'RECVD_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[18],'LATE_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[19],'EXCESS_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[20],'SHORT_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[21],'ORDER_COST_GROSS') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[22],'ORDER_COST_ACTUAL') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[23],'ORDER_COST_NET') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[24],'FREIGHT_COST') \
	.withColumnRenamed(SQ_Shortcut_To_PURCHASES.columns[25],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_To_PURCH_PERF, type TARGET 
# COLUMN COUNT: 26


Shortcut_To_PURCH_PERF = SQ_Shortcut_To_PURCHASES.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(SAP_CATEGORY_ID AS INT) as SAP_CATEGORY_ID",
	"VENDOR_ID as VENDOR_ID",
	"CAST(PO_TYPE_ID AS STRING) as PO_TYPE_ID",
	"DISTRICT_ID as DISTRICT_ID",
	"CAST(STORE_TYPE_ID AS STRING) as STORE_TYPE_ID",
	"CAST(RECVD_AFTER_DUE_DAYS_CNT AS INT) as RECVD_AFTER_DUE_DAYS_CNT",
	"CAST(RECVD_BEFORE_DUE_DAYS_CNT AS INT) as RECVD_BEFORE_DUE_DAYS_CNT",
	"CAST(ACTUAL_LEADTIME_DAYS_CNT AS INT) as ACTUAL_LEADTIME_DAYS_CNT",
	"CAST(PO_DUE_CNT AS INT) as PO_DUE_CNT",
	"CAST(PO_RECVD_CNT AS INT) as PO_RECVD_CNT",
	"CAST(PO_LATE_CNT AS INT) as PO_LATE_CNT",
	"CAST(LINE_ITEM_DUE_CNT AS INT) as LINE_ITEM_DUE_CNT",
	"CAST(LINE_ITEM_RECVD_CNT AS INT) as LINE_ITEM_RECVD_CNT",
	"CAST(LINE_ITEMS_LATE_CNT AS INT) as LINE_ITEMS_LATE_CNT",
	"CAST(LINE_ITEM_OOB_CNT AS INT) as LINE_ITEM_OOB_CNT",
	"CAST(DUE_QTY AS INT) as DUE_QTY",
	"CAST(RECVD_QTY AS INT) as RECVD_QTY",
	"CAST(LATE_QTY AS INT) as LATE_QTY",
	"CAST(EXCESS_QTY AS INT) as EXCESS_QTY",
	"CAST(SHORT_QTY AS INT) as SHORT_QTY",
	"CAST(ORDER_COST_GROSS AS DECIMAL(12,2)) as ORDER_COST_GROSS",
	"CAST(ORDER_COST_ACTUAL AS DECIMAL(12,2)) as ORDER_COST_ACTUAL",
	"CAST(ORDER_COST_NET AS DECIMAL(12,2)) as ORDER_COST_NET",
	"CAST(FREIGHT_COST AS DECIMAL(12,2)) as FREIGHT_COST",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
Shortcut_To_PURCH_PERF.write.mode('overwrite').saveAsTable(f'{legacy}.PURCH_PERF')

# COMMAND ----------


