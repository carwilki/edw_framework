# Databricks notebook source
#Code converted on 2023-08-22 15:46:24
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
dbutils.widgets.text(name = 'env', defaultValue = '')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE = spark.sql(f"""SELECT
DAY_DATE,
LOCATION_CODE,
PRODUCT_CODE,
SUPPLIER_CODE,
PROJECTED_ORDER_PROPOSALS,
PROJECTED_DELIVERIES
FROM {raw}.RELEX_DAILY_ORDER_PROJECTION_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_VENDOR_PROFILE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_VENDOR_PROFILE = spark.sql(f"""SELECT
VENDOR_ID,
VENDOR_TYPE_ID,
VENDOR_NBR
FROM {legacy}.VENDOR_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_SUPPLIER_CODE, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE_temp = SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE.toDF(*["SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___" + col for col in SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE.columns])

EXP_SUPPLIER_CODE = SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE_temp.selectExpr(
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___DAY_DATE as DAY_DATE",
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___LOCATION_CODE as LOCATION_CODE",
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___PRODUCT_CODE as PRODUCT_CODE",
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___SUPPLIER_CODE as SUPPLIER_CODE",
	"SUBSTR ( SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___SUPPLIER_CODE , 1 , IF (INSTR ( SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___SUPPLIER_CODE , '-' ) = 0, LENGTH(SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___SUPPLIER_CODE), INSTR ( SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___SUPPLIER_CODE , '-' ) - 1) ) as o_jnr_SUPPLIER_CODE",
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___PROJECTED_ORDER_PROPOSALS as PROJECTED_ORDER_PROPOSALS",
	"SQ_Shortcut_to_RELEX_DAILY_ORDER_PROJECTION_PRE___PROJECTED_DELIVERIES as PROJECTED_DELIVERIES"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT1, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT1 = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_VENDOR_TYPE_ID, type FILTER 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_VENDOR_PROFILE_temp = SQ_Shortcut_to_VENDOR_PROFILE.toDF(*["SQ_Shortcut_to_VENDOR_PROFILE___" + col for col in SQ_Shortcut_to_VENDOR_PROFILE.columns])

FIL_VENDOR_TYPE_ID = SQ_Shortcut_to_VENDOR_PROFILE_temp.selectExpr(
	"SQ_Shortcut_to_VENDOR_PROFILE___VENDOR_ID as VENDOR_ID",
	"SQ_Shortcut_to_VENDOR_PROFILE___VENDOR_TYPE_ID as VENDOR_TYPE_ID",
	"SQ_Shortcut_to_VENDOR_PROFILE___VENDOR_NBR as VENDOR_NBR").filter("VENDOR_TYPE_ID != 3 AND VENDOR_TYPE_ID != 4 AND VENDOR_TYPE_ID != 5 AND VENDOR_TYPE_ID != 6 AND VENDOR_TYPE_ID != 7 AND VENDOR_TYPE_ID != 8 AND VENDOR_TYPE_ID != 9").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
LOCATION_ID,
LOCATION_TYPE_ID,
LOCATION_NBR
FROM {legacy}.SITE_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_LOCATION_TYPE_ID, type FILTER 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])

FIL_LOCATION_TYPE_ID = SQ_Shortcut_to_SITE_PROFILE_RPT_temp.selectExpr(
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_NBR as LOCATION_NBR").filter("LOCATION_TYPE_ID = 1 OR LOCATION_TYPE_ID = 2 OR LOCATION_TYPE_ID = 3 OR LOCATION_TYPE_ID = 19").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SITE_PROFILE_RPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_SUPPLIER_CODE_temp = EXP_SUPPLIER_CODE.toDF(*["EXP_SUPPLIER_CODE___" + col for col in EXP_SUPPLIER_CODE.columns])
FIL_LOCATION_TYPE_ID_temp = FIL_LOCATION_TYPE_ID.toDF(*["FIL_LOCATION_TYPE_ID___" + col for col in FIL_LOCATION_TYPE_ID.columns])

JNR_SITE_PROFILE_RPT = FIL_LOCATION_TYPE_ID_temp.join(EXP_SUPPLIER_CODE_temp,[FIL_LOCATION_TYPE_ID_temp.FIL_LOCATION_TYPE_ID___LOCATION_NBR == EXP_SUPPLIER_CODE_temp.EXP_SUPPLIER_CODE___o_jnr_SUPPLIER_CODE],'inner').selectExpr(
	"EXP_SUPPLIER_CODE___DAY_DATE as DAY_DATE",
	"EXP_SUPPLIER_CODE___LOCATION_CODE as LOCATION_CODE",
	"EXP_SUPPLIER_CODE___PRODUCT_CODE as PRODUCT_CODE",
	"EXP_SUPPLIER_CODE___SUPPLIER_CODE as SUPPLIER_CODE",
	"EXP_SUPPLIER_CODE___o_jnr_SUPPLIER_CODE as jnr_SUPPLIER_CODE",
	"EXP_SUPPLIER_CODE___PROJECTED_ORDER_PROPOSALS as PROJECTED_ORDER_PROPOSALS",
	"EXP_SUPPLIER_CODE___PROJECTED_DELIVERIES as PROJECTED_DELIVERIES",
	"FIL_LOCATION_TYPE_ID___LOCATION_ID as LOCATION_ID",
	"FIL_LOCATION_TYPE_ID___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"FIL_LOCATION_TYPE_ID___LOCATION_NBR as LOCATION_NBR")

# COMMAND ----------

# Processing node EXP_LOCATION_NBR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_RPT_temp = JNR_SITE_PROFILE_RPT.toDF(*["JNR_SITE_PROFILE_RPT___" + col for col in JNR_SITE_PROFILE_RPT.columns])

# .selectExpr(
# 	"JNR_SITE_PROFILE_RPT___DAY_DATE as DAY_DATE",
# 	"JNR_SITE_PROFILE_RPT___LOCATION_CODE as LOCATION_CODE",
# 	"JNR_SITE_PROFILE_RPT___PRODUCT_CODE as PRODUCT_CODE",
# 	"JNR_SITE_PROFILE_RPT___SUPPLIER_CODE as SUPPLIER_CODE",
# 	"JNR_SITE_PROFILE_RPT___PROJECTED_ORDER_PROPOSALS as PROJECTED_ORDER_PROPOSALS",
# 	"JNR_SITE_PROFILE_RPT___PROJECTED_DELIVERIES as PROJECTED_DELIVERIES",
# 	"JNR_SITE_PROFILE_RPT___LOCATION_ID as FROM_LOCATION_ID",
# 	"JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
# 	"JNR_SITE_PROFILE_RPT___LOCATION_NBR as LOCATION_NBR").

EXP_LOCATION_NBR = JNR_SITE_PROFILE_RPT_temp.selectExpr(
	# "JNR_SITE_PROFILE_RPT___sys_row_id as sys_row_id",
	"JNR_SITE_PROFILE_RPT___DAY_DATE as DAY_DATE",
	"JNR_SITE_PROFILE_RPT___LOCATION_CODE as LOCATION_CODE",
	"JNR_SITE_PROFILE_RPT___PRODUCT_CODE as PRODUCT_CODE",
	"JNR_SITE_PROFILE_RPT___SUPPLIER_CODE as SUPPLIER_CODE",
	"JNR_SITE_PROFILE_RPT___PROJECTED_ORDER_PROPOSALS as PROJECTED_ORDER_PROPOSALS",
	"JNR_SITE_PROFILE_RPT___PROJECTED_DELIVERIES as PROJECTED_DELIVERIES",
	"JNR_SITE_PROFILE_RPT___LOCATION_ID as FROM_LOCATION_ID",
	"IF (JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID = 1 OR JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID = 2 OR JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID = 3, concat( 'V' , JNR_SITE_PROFILE_RPT___LOCATION_NBR ), JNR_SITE_PROFILE_RPT___LOCATION_NBR) as o_LOCATION_NBR"
)

# COMMAND ----------

# Processing node JNR_VENDOR_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_LOCATION_NBR_temp = EXP_LOCATION_NBR.toDF(*["EXP_LOCATION_NBR___" + col for col in EXP_LOCATION_NBR.columns])
FIL_VENDOR_TYPE_ID_temp = FIL_VENDOR_TYPE_ID.toDF(*["FIL_VENDOR_TYPE_ID___" + col for col in FIL_VENDOR_TYPE_ID.columns])

JNR_VENDOR_PROFILE = FIL_VENDOR_TYPE_ID_temp.join(EXP_LOCATION_NBR_temp,[FIL_VENDOR_TYPE_ID_temp.FIL_VENDOR_TYPE_ID___VENDOR_NBR == EXP_LOCATION_NBR_temp.EXP_LOCATION_NBR___o_LOCATION_NBR],'inner').selectExpr(
	"EXP_LOCATION_NBR___DAY_DATE as DAY_DATE",
	"EXP_LOCATION_NBR___LOCATION_CODE as LOCATION_CODE",
	"EXP_LOCATION_NBR___PRODUCT_CODE as PRODUCT_CODE",
	"EXP_LOCATION_NBR___SUPPLIER_CODE as SUPPLIER_CODE",
	"EXP_LOCATION_NBR___PROJECTED_ORDER_PROPOSALS as PROJECTED_ORDER_PROPOSALS",
	"EXP_LOCATION_NBR___PROJECTED_DELIVERIES as PROJECTED_DELIVERIES",
	"EXP_LOCATION_NBR___FROM_LOCATION_ID as FROM_LOCATION_ID",
	"EXP_LOCATION_NBR___o_LOCATION_NBR as LOCATION_NBR",
	"FIL_VENDOR_TYPE_ID___VENDOR_ID as VENDOR_ID",
	"FIL_VENDOR_TYPE_ID___VENDOR_NBR as VENDOR_NBR")

# COMMAND ----------

# Processing node JNR_SITE_PROFILE_RPT_1, type JOINER 
# COLUMN COUNT: 10

JNR_SITE_PROFILE_RPT_1 = SQ_Shortcut_to_SITE_PROFILE_RPT1.join(JNR_VENDOR_PROFILE,[SQ_Shortcut_to_SITE_PROFILE_RPT1.STORE_NBR == JNR_VENDOR_PROFILE.LOCATION_CODE],'inner')

# COMMAND ----------

# Processing node JNR_SKU_PROFILE_RPT, type JOINER 
# COLUMN COUNT: 10

JNR_SKU_PROFILE_RPT = SQ_Shortcut_to_SKU_PROFILE_RPT.join(JNR_SITE_PROFILE_RPT_1,[SQ_Shortcut_to_SKU_PROFILE_RPT.SKU_NBR == JNR_SITE_PROFILE_RPT_1.PRODUCT_CODE],'inner')

# COMMAND ----------

# Processing node EXP_VENDOR_SUB_GROUP, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_RPT_temp = JNR_SKU_PROFILE_RPT.toDF(*["JNR_SKU_PROFILE_RPT___" + col for col in JNR_SKU_PROFILE_RPT.columns])

EXP_VENDOR_SUB_GROUP = JNR_SKU_PROFILE_RPT_temp.selectExpr(
	# "JNR_SKU_PROFILE_RPT___sys_row_id as sys_row_id",
	"CURRENT_TIMESTAMP as o_SNAPSHOT_DT",
	"JNR_SKU_PROFILE_RPT___DAY_DATE as DAY_DATE",
	"IF (INSTR ( JNR_SKU_PROFILE_RPT___SUPPLIER_CODE , '-' ) = 0, NULL, SUBSTR ( JNR_SKU_PROFILE_RPT___SUPPLIER_CODE , INSTR ( JNR_SKU_PROFILE_RPT___SUPPLIER_CODE , '-' ) + 1 , LENGTH(JNR_SKU_PROFILE_RPT___SUPPLIER_CODE) - INSTR ( JNR_SKU_PROFILE_RPT___SUPPLIER_CODE , '-') )) as o_SOURCE_VENDOR_SUB_GROUP",
	"JNR_SKU_PROFILE_RPT___PROJECTED_ORDER_PROPOSALS as PROJECTED_ORDER_PROPOSALS",
	"JNR_SKU_PROFILE_RPT___PROJECTED_DELIVERIES as PROJECTED_DELIVERIES",
	"JNR_SKU_PROFILE_RPT___FROM_LOCATION_ID as FROM_LOCATION_ID",
	"JNR_SKU_PROFILE_RPT___VENDOR_ID as VENDOR_ID",
	"JNR_SKU_PROFILE_RPT___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_DP_ORDER_PROJECTION_DAY_HIST, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_DP_ORDER_PROJECTION_DAY_HIST = EXP_VENDOR_SUB_GROUP.selectExpr(
	"CAST(o_SNAPSHOT_DT AS DATE) as SNAPSHOT_DT",
	"CAST(DAY_DATE AS DATE) as DAY_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(FROM_LOCATION_ID AS INT) as FROM_LOCATION_ID",
	"CAST(VENDOR_ID AS INT) as SOURCE_VENDOR_ID",
	"CAST(o_SOURCE_VENDOR_SUB_GROUP AS STRING) as SOURCE_VENDOR_SUB_GROUP",
	"CAST(PROJECTED_ORDER_PROPOSALS AS DECIMAL(12,4)) as PROJECTED_ORDER_PROPOSAL_QTY",
	"CAST(PROJECTED_DELIVERIES AS DECIMAL(12,4)) as PROJECTED_DELIVERY_QTY",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_DP_ORDER_PROJECTION_DAY_HIST.write.saveAsTable(f'{legacy}.DP_ORDER_PROJECTION_DAY_HIST', mode = 'append')
logPrevRunDt("DP_ORDER_PROJECTION_DAY_HIST", "DP_ORDER_PROJECTION_DAY_HIST", "Completed", "N/A", f"{raw}.log_run_details")