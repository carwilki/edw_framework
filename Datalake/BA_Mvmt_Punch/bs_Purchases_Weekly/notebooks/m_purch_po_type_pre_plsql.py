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
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

spark.sql(f'truncate table {raw}.PURCH_PO_TYPE_PRE ')

# COMMAND ----------

spark.sql(f"""
          INSERT INTO {raw}.PURCH_PO_TYPE_PRE 
          SELECT 
          DAYS.WEEK_DT, 
          ITEM.SAP_CATEGORY_ID, 
          PUR.VENDOR_ID, 
          PUR.PO_TYPE_ID, 
          SITE.DISTRICT_ID, 
          SITE.STORE_TYPE_ID, 
          SUM(CASE WHEN ORIG_DATE_DUE < DATE_RECEIVED THEN TO_NUMBER(date_diff(DATE_RECEIVED , ORIG_DATE_DUE),9999999999.999999) ELSE 0 END) AS RECVD_AFTER_DUE_DAYS_CNT , 
          SUM(CASE WHEN ORIG_DATE_DUE > DATE_RECEIVED THEN TO_NUMBER(date_diff(ORIG_DATE_DUE , DATE_RECEIVED),9999999999.999999) ELSE 0 END) AS RECVD_BEFORE_DUE_DAYS_CNT , 
          CAST(SUM(date_diff(DATE_RECEIVED , PUR.DAY_DT)) AS INTEGER) ACTUAL_LEADTIME_DAYS_CNT, 
          COUNT(PO_LINE_NBR) LINE_ITEM_DUE_CNT, SUM(CASE WHEN RECEIPTS_QTY = 0 THEN 0 ELSE 1 END) AS LINE_ITEM_RECVD_CNT , 
          SUM(CASE WHEN ORIG_DATE_DUE < DATE_RECEIVED THEN 1 ELSE 0 END) AS 
          LINE_ITEM_LATE_CNT , 
          SUM(CASE WHEN (ORIG_ORDER_QTY - RECEIPTS_QTY) = 0 THEN 0 ELSE 1 END) AS LINE_ITEM_OOB_CNT , 
          SUM(PUR.ORIG_ORDER_QTY) DUE_QTY, 
          SUM(CASE WHEN PUR.RECEIPTS_QTY = 0 THEN 0 ELSE PUR.ORIG_ORDER_QTY END) AS RECVD_QTY , 
          SUM(CASE WHEN ORIG_DATE_DUE < DATE_RECEIVED THEN ORIG_ORDER_QTY ELSE 0 END) AS LATE_QTY , 
          SUM(CASE WHEN (RECEIPTS_QTY - ORIG_ORDER_QTY) > 0 THEN (RECEIPTS_QTY - ORIG_ORDER_QTY) ELSE 0 END) AS EXCESS_QTY , 
          SUM(CASE WHEN (RECEIPTS_QTY - ORIG_ORDER_QTY) < 0 THEN (ORIG_ORDER_QTY - RECEIPTS_QTY) ELSE 0 END) AS SHORT_QTY , 
          SUM(PUR.ORDER_COST_GROSS) ORDER_COST_GROSS, 
          SUM(PUR.ORDER_COST_ACTUAL) ORDER_COST_ACTUAL, 
          SUM(PUR.ORDER_COST_NET) ORDER_COST_NET, 
          SUM(PUR.FREIGHT_COST) FREIGHT_COST, 
          CURRENT_TIMESTAMP AS load_dt 
          from 
          ( SELECT DAY_DT , PRODUCT_ID, VENDOR_ID, PO_TYPE_ID , ORIG_DATE_DUE, DATE_RECEIVED , PO_LINE_NBR , LOCATION_ID, ORIG_ORDER_QTY , RECEIPTS_QTY, ORDER_COST_GROSS, ORDER_COST_ACTUAL , ORDER_COST_NET , FREIGHT_COST 
            FROM {legacy}.PURCHASES 
            WHERE DELETE_FLAG = 0 
            AND PO_TYPE_ID IN ('NB','RB') 
            AND ORIG_DATE_DUE > (current_date - 740 - (date_part('dow', current_date - 740)-1))+1 ) PUR, 
        ( SELECT PRODUCT_ID, SAP_CATEGORY_ID FROM {legacy}.SKU_PROFILE ) ITEM, 
        ( SELECT LOCATION_ID, DISTRICT_ID, STORE_TYPE_ID FROM {legacy}.SITE_PROFILE ) SITE, 
        {enterprise}.DAYS WHERE PUR.PRODUCT_ID = ITEM.PRODUCT_ID 
        AND PUR.LOCATION_ID = SITE.LOCATION_ID 
        AND PUR.ORIG_DATE_DUE = DAYS.DAY_DT 
        GROUP BY DAYS.WEEK_DT, ITEM.SAP_CATEGORY_ID, PUR.VENDOR_ID, PUR.PO_TYPE_ID , SITE.DISTRICT_ID, SITE.STORE_TYPE_ID
""")

# COMMAND ----------

# # Processing node SQ_Shortcut_To_INVENTORY_PRE, type SOURCE 
# # COLUMN COUNT: 2

# SQ_Shortcut_To_INVENTORY_PRE = spark.read.csv('', sep=',', header='false').withColumn("sys_row_id", monotonically_increasing_id())
# # Conforming fields names to the component layout
# SQ_Shortcut_To_INVENTORY_PRE = SQ_Shortcut_To_INVENTORY_PRE \
# 	.withColumnRenamed(SQ_Shortcut_To_INVENTORY_PRE.columns[0],'') \
# 	.withColumnRenamed(SQ_Shortcut_To_INVENTORY_PRE.columns[1],'JOB_NAME')

# COMMAND ----------

# # Processing node Shortcut_to_mplt_GENERIC_SQL, type MAPPLET 
# # COLUMN COUNT: 0


# # Constructing dataframe for input into mapplet mplt_GENERIC_SQL, input group INP_MPLT_GENERIC_SQL
# Shortcut_to_mplt_GENERIC_SQL_INP_MPLT_GENERIC_SQL = SQ_Shortcut_To_INVENTORY_PRE.select(
# 	SQ_Shortcut_To_INVENTORY_PRE.JOB_NAME.alias('MAP_NAME'),
# 	SQ_Shortcut_To_INVENTORY_PRE.sys_row_id.alias('sys_row_id'))
# Shortcut_to_mplt_GENERIC_SQL = Mapplets.mplt_GENERIC_SQL(Shortcut_to_mplt_GENERIC_SQL_INP_MPLT_GENERIC_SQL)

# COMMAND ----------

# # Processing node Shortcut_to_EXP_COMMON_PLSQL_ABORT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 2

# # for each involved DataFrame, append the dataframe name to each column
# Shortcut_to_mplt_GENERIC_SQL_temp = Shortcut_to_mplt_GENERIC_SQL.toDF(*["Shortcut_to_mplt_GENERIC_SQL___" + col for col in Shortcut_to_mplt_GENERIC_SQL.columns])

# Shortcut_to_EXP_COMMON_PLSQL_ABORT = Shortcut_to_mplt_GENERIC_SQL_temp.selectExpr(
# 	"Shortcut_to_mplt_GENERIC_SQL___MAP_NAME1 as io_map_name",
# 	"Shortcut_to_mplt_GENERIC_SQL___MPLT_STATUS as in_result").selectExpr(
# 	"Shortcut_to_mplt_GENERIC_SQL___sys_row_id as sys_row_id",
# 	"Shortcut_to_mplt_GENERIC_SQL___io_map_name as io_map_name",
# 	"IF (Shortcut_to_mplt_GENERIC_SQL___in_result > 0, ABORT ( 'sess_failed' ), 'SUCCESS') as abort")

# COMMAND ----------

# # Processing node Shortcut_To_QUERY_ARGUMENTS1, type TARGET 
# # COLUMN COUNT: 8


# Shortcut_To_QUERY_ARGUMENTS1 = Shortcut_to_EXP_COMMON_PLSQL_ABORT.selectExpr(
# 	"CAST(io_map_name AS STRING) as JOB_NAME",
# 	"col(\"LAST_CHANGE_DT\") as LAST_CHANGE_DT",
# 	"CAST(lit(None) AS STRING) as LAST_CHANGE_USER_ID",
# 	"CAST(lit(None) AS STRING) as TABLE_NAME",
# 	"CAST(abort AS STRING) as SQL_DESC",
# 	"CAST(lit(None) AS STRING) as SQL_TX",
# 	"CAST(lit(None) AS STRING) as SQL_TX2",
# 	"CAST(lit(None) AS STRING) as SQL_TX3"
# )
# Shortcut_To_QUERY_ARGUMENTS1.write.saveAsTable(f'{raw}.QUERY_ARGUMENTS')
