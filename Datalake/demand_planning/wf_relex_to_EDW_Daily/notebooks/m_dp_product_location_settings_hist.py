# Databricks notebook source
#Code converted on 2023-08-22 15:46:26
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

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT1, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT1 = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT2, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT2 = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT3, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT3 = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_VENDOR_PROFILE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_VENDOR_PROFILE = spark.sql(f"""SELECT
VENDOR_ID,
VENDOR_TYPE_ID,
VENDOR_NBR
FROM {legacy}.VENDOR_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT1, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT1 = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE = spark.sql(f"""SELECT
LOCATION_CODE,
PRODUCT_CODE,
SUPPLIER_CODE,
PURCHASE_GROUP,
INTRODUCTION_DATE,
TERMINATION_DATE,
REPLACES_PRODUCT,
REPLACING_PRODUCT,
REFERENCE_PRODUCT,
REFERENCE_SCALING_FACTOR,
ORDER_MODEL,
FORECAST_MODEL,
ABC_CLASS,
PRODUCT_ABC_CLASS,
XYZ_CLASS,
PRODUCT_XYZ_CLASS,
DEMAND_SATISFIED_PCT
FROM {raw}.RELEX_PRODUCT_LOCATION_SETTINGS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

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

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
LOCATION_TYPE_ID,
LOCATION_NBR
FROM {legacy}.SITE_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE = spark.sql(f"""SELECT
LOCATION_CODE,
PRODUCT_CODE,
END_BALANCE,
ORDER_PARAMETER_QTY,
MIN_FILL,
ORDER_BATCH_SIZE,
ECONOMIC_ORDER_QTY,
MIN_DELIVERY_QTY
FROM {raw}.RELEX_INVENTORY_LAYER_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_RELEX_INVENTORY_LAYER_PRE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 25

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE_temp = SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE.toDF(*["SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___" + col for col in SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE.columns])
SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE_temp = SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE.toDF(*["SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___" + col for col in SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE.columns])

JNR_RELEX_INVENTORY_LAYER_PRE = SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE_temp.join(SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE_temp,[SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE_temp.SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___LOCATION_CODE == SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE_temp.SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___LOCATION_CODE, SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE_temp.SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___PRODUCT_CODE == SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE_temp.SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___PRODUCT_CODE],'inner').selectExpr(
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___LOCATION_CODE as LOCATION_CODE",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___PRODUCT_CODE as PRODUCT_CODE",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___SUPPLIER_CODE as SUPPLIER_CODE",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___PURCHASE_GROUP as PURCHASE_GROUP",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___TERMINATION_DATE as TERMINATION_DATE",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___REPLACES_PRODUCT as REPLACES_PRODUCT",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___ORDER_MODEL as ORDER_MODEL",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___FORECAST_MODEL as FORECAST_MODEL",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___ABC_CLASS as ABC_CLASS",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___XYZ_CLASS as XYZ_CLASS",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"SQ_Shortcut_to_RELEX_PRODUCT_LOCATION_SETTINGS_PRE___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___LOCATION_CODE as inv_layer_LOCATION_CODE",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___PRODUCT_CODE as inv_layer_PRODUCT_CODE",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___END_BALANCE as END_BALANCE",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___MIN_FILL as MIN_FILL",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"SQ_Shortcut_to_RELEX_INVENTORY_LAYER_PRE___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY")

# COMMAND ----------

# Processing node FIL_LOCATION_TYPE_ID, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])

FIL_LOCATION_TYPE_ID = SQ_Shortcut_to_SITE_PROFILE_RPT_temp.selectExpr(
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_NBR as LOCATION_NBR").filter("LOCATION_TYPE_ID = 1 OR LOCATION_TYPE_ID = 2 OR LOCATION_TYPE_ID = 3 OR LOCATION_TYPE_ID = 19").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_SUPPLIER_CODE, type EXPRESSION 
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
JNR_RELEX_INVENTORY_LAYER_PRE_temp = JNR_RELEX_INVENTORY_LAYER_PRE.toDF(*["JNR_RELEX_INVENTORY_LAYER_PRE___" + col for col in JNR_RELEX_INVENTORY_LAYER_PRE.columns])

EXP_SUPPLIER_CODE = JNR_RELEX_INVENTORY_LAYER_PRE_temp.selectExpr(
	# "JNR_RELEX_INVENTORY_LAYER_PRE___sys_row_id as sys_row_id",
	"JNR_RELEX_INVENTORY_LAYER_PRE___LOCATION_CODE as LOCATION_CODE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___PRODUCT_CODE as PRODUCT_CODE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___SUPPLIER_CODE as SUPPLIER_CODE",
	"SUBSTR ( JNR_RELEX_INVENTORY_LAYER_PRE___SUPPLIER_CODE , 1 , IF (INSTR ( JNR_RELEX_INVENTORY_LAYER_PRE___SUPPLIER_CODE , '-' ) = 0, LENGTH(JNR_RELEX_INVENTORY_LAYER_PRE___SUPPLIER_CODE), INSTR ( JNR_RELEX_INVENTORY_LAYER_PRE___SUPPLIER_CODE , '-' ) - 1) ) as o_jnr_SUPPLIER_CODE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___PURCHASE_GROUP as PURCHASE_GROUP",
	"JNR_RELEX_INVENTORY_LAYER_PRE___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___TERMINATION_DATE as TERMINATION_DATE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___REPLACES_PRODUCT as REPLACES_PRODUCT",
	"JNR_RELEX_INVENTORY_LAYER_PRE___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"JNR_RELEX_INVENTORY_LAYER_PRE___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"JNR_RELEX_INVENTORY_LAYER_PRE___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"JNR_RELEX_INVENTORY_LAYER_PRE___ORDER_MODEL as ORDER_MODEL",
	"JNR_RELEX_INVENTORY_LAYER_PRE___FORECAST_MODEL as FORECAST_MODEL",
	"JNR_RELEX_INVENTORY_LAYER_PRE___ABC_CLASS as ABC_CLASS",
	"JNR_RELEX_INVENTORY_LAYER_PRE___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"JNR_RELEX_INVENTORY_LAYER_PRE___XYZ_CLASS as XYZ_CLASS",
	"JNR_RELEX_INVENTORY_LAYER_PRE___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"JNR_RELEX_INVENTORY_LAYER_PRE___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"JNR_RELEX_INVENTORY_LAYER_PRE___END_BALANCE as END_BALANCE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"JNR_RELEX_INVENTORY_LAYER_PRE___MIN_FILL as MIN_FILL",
	"JNR_RELEX_INVENTORY_LAYER_PRE___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"JNR_RELEX_INVENTORY_LAYER_PRE___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"JNR_RELEX_INVENTORY_LAYER_PRE___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY"
)

# COMMAND ----------

# Processing node JNR_SITE_PROFILE_RPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
EXP_SUPPLIER_CODE_temp = EXP_SUPPLIER_CODE.toDF(*["EXP_SUPPLIER_CODE___" + col for col in EXP_SUPPLIER_CODE.columns])
FIL_LOCATION_TYPE_ID_temp = FIL_LOCATION_TYPE_ID.toDF(*["FIL_LOCATION_TYPE_ID___" + col for col in FIL_LOCATION_TYPE_ID.columns])

JNR_SITE_PROFILE_RPT = FIL_LOCATION_TYPE_ID_temp.join(EXP_SUPPLIER_CODE_temp,[FIL_LOCATION_TYPE_ID_temp.FIL_LOCATION_TYPE_ID___LOCATION_NBR == EXP_SUPPLIER_CODE_temp.EXP_SUPPLIER_CODE___o_jnr_SUPPLIER_CODE],'inner').selectExpr(
	"EXP_SUPPLIER_CODE___LOCATION_CODE as LOCATION_CODE",
	"EXP_SUPPLIER_CODE___PRODUCT_CODE as PRODUCT_CODE",
	"EXP_SUPPLIER_CODE___SUPPLIER_CODE as SUPPLIER_CODE",
	"EXP_SUPPLIER_CODE___o_jnr_SUPPLIER_CODE as jnr_SUPPLIER_CODE",
	"EXP_SUPPLIER_CODE___PURCHASE_GROUP as PURCHASE_GROUP",
	"EXP_SUPPLIER_CODE___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"EXP_SUPPLIER_CODE___TERMINATION_DATE as TERMINATION_DATE",
	"EXP_SUPPLIER_CODE___REPLACES_PRODUCT as REPLACES_PRODUCT",
	"EXP_SUPPLIER_CODE___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"EXP_SUPPLIER_CODE___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"EXP_SUPPLIER_CODE___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"EXP_SUPPLIER_CODE___ORDER_MODEL as ORDER_MODEL",
	"EXP_SUPPLIER_CODE___FORECAST_MODEL as FORECAST_MODEL",
	"EXP_SUPPLIER_CODE___ABC_CLASS as ABC_CLASS",
	"EXP_SUPPLIER_CODE___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"EXP_SUPPLIER_CODE___XYZ_CLASS as XYZ_CLASS",
	"EXP_SUPPLIER_CODE___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"EXP_SUPPLIER_CODE___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"EXP_SUPPLIER_CODE___END_BALANCE as END_BALANCE",
	"EXP_SUPPLIER_CODE___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"EXP_SUPPLIER_CODE___MIN_FILL as MIN_FILL",
	"EXP_SUPPLIER_CODE___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"FIL_LOCATION_TYPE_ID___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"FIL_LOCATION_TYPE_ID___LOCATION_NBR as LOCATION_NBR",
	"EXP_SUPPLIER_CODE___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"EXP_SUPPLIER_CODE___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY")

# COMMAND ----------

# Processing node EXP_LOCATION_NBR, type EXPRESSION 
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_RPT_temp = JNR_SITE_PROFILE_RPT.toDF(*["JNR_SITE_PROFILE_RPT___" + col for col in JNR_SITE_PROFILE_RPT.columns])

EXP_LOCATION_NBR = JNR_SITE_PROFILE_RPT_temp.selectExpr(
	# "JNR_SITE_PROFILE_RPT___sys_row_id as sys_row_id",
	"JNR_SITE_PROFILE_RPT___LOCATION_CODE as LOCATION_CODE",
	"JNR_SITE_PROFILE_RPT___PRODUCT_CODE as PRODUCT_CODE",
	"JNR_SITE_PROFILE_RPT___SUPPLIER_CODE as SUPPLIER_CODE",
	"JNR_SITE_PROFILE_RPT___PURCHASE_GROUP as PURCHASE_GROUP",
	"JNR_SITE_PROFILE_RPT___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"JNR_SITE_PROFILE_RPT___TERMINATION_DATE as TERMINATION_DATE",
	"JNR_SITE_PROFILE_RPT___REPLACES_PRODUCT as REPLACES_PRODUCT",
	"JNR_SITE_PROFILE_RPT___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"JNR_SITE_PROFILE_RPT___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"JNR_SITE_PROFILE_RPT___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"JNR_SITE_PROFILE_RPT___ORDER_MODEL as ORDER_MODEL",
	"JNR_SITE_PROFILE_RPT___FORECAST_MODEL as FORECAST_MODEL",
	"JNR_SITE_PROFILE_RPT___ABC_CLASS as ABC_CLASS",
	"JNR_SITE_PROFILE_RPT___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"JNR_SITE_PROFILE_RPT___XYZ_CLASS as XYZ_CLASS",
	"JNR_SITE_PROFILE_RPT___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"JNR_SITE_PROFILE_RPT___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"JNR_SITE_PROFILE_RPT___END_BALANCE as END_BALANCE",
	"JNR_SITE_PROFILE_RPT___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"JNR_SITE_PROFILE_RPT___MIN_FILL as MIN_FILL",
	"JNR_SITE_PROFILE_RPT___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"IF (JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID = 1 OR JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID = 2 OR JNR_SITE_PROFILE_RPT___LOCATION_TYPE_ID = 3, concat( 'V' , JNR_SITE_PROFILE_RPT___LOCATION_NBR ), JNR_SITE_PROFILE_RPT___LOCATION_NBR) as o_LOCATION_NBR",
	"JNR_SITE_PROFILE_RPT___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"JNR_SITE_PROFILE_RPT___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY"
)

# COMMAND ----------

# Processing node JNR_VENDOR_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
EXP_LOCATION_NBR_temp = EXP_LOCATION_NBR.toDF(*["EXP_LOCATION_NBR___" + col for col in EXP_LOCATION_NBR.columns])
FIL_VENDOR_TYPE_ID_temp = FIL_VENDOR_TYPE_ID.toDF(*["FIL_VENDOR_TYPE_ID___" + col for col in FIL_VENDOR_TYPE_ID.columns])

JNR_VENDOR_PROFILE = FIL_VENDOR_TYPE_ID_temp.join(EXP_LOCATION_NBR_temp,[FIL_VENDOR_TYPE_ID_temp.FIL_VENDOR_TYPE_ID___VENDOR_NBR == EXP_LOCATION_NBR_temp.EXP_LOCATION_NBR___o_LOCATION_NBR],'inner').selectExpr(
	"EXP_LOCATION_NBR___LOCATION_CODE as LOCATION_CODE",
	"EXP_LOCATION_NBR___PRODUCT_CODE as PRODUCT_CODE",
	"EXP_LOCATION_NBR___SUPPLIER_CODE as SUPPLIER_CODE",
	"EXP_LOCATION_NBR___PURCHASE_GROUP as PURCHASE_GROUP",
	"EXP_LOCATION_NBR___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"EXP_LOCATION_NBR___TERMINATION_DATE as TERMINATION_DATE",
	"EXP_LOCATION_NBR___REPLACES_PRODUCT as REPLACES_PRODUCT",
	"EXP_LOCATION_NBR___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"EXP_LOCATION_NBR___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"EXP_LOCATION_NBR___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"EXP_LOCATION_NBR___ORDER_MODEL as ORDER_MODEL",
	"EXP_LOCATION_NBR___FORECAST_MODEL as FORECAST_MODEL",
	"EXP_LOCATION_NBR___ABC_CLASS as ABC_CLASS",
	"EXP_LOCATION_NBR___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"EXP_LOCATION_NBR___XYZ_CLASS as XYZ_CLASS",
	"EXP_LOCATION_NBR___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"EXP_LOCATION_NBR___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"EXP_LOCATION_NBR___END_BALANCE as END_BALANCE",
	"EXP_LOCATION_NBR___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"EXP_LOCATION_NBR___MIN_FILL as MIN_FILL",
	"EXP_LOCATION_NBR___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"EXP_LOCATION_NBR___o_LOCATION_NBR as LOCATION_NBR",
	"FIL_VENDOR_TYPE_ID___VENDOR_ID as VENDOR_ID",
	"FIL_VENDOR_TYPE_ID___VENDOR_NBR as VENDOR_NBR",
	"EXP_LOCATION_NBR___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"EXP_LOCATION_NBR___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY")

# COMMAND ----------

# Processing node JNR_SITE_PROFILE_RPT_1, type JOINER 
# COLUMN COUNT: 26

JNR_SITE_PROFILE_RPT_1 = SQ_Shortcut_to_SITE_PROFILE_RPT1.join(JNR_VENDOR_PROFILE,[SQ_Shortcut_to_SITE_PROFILE_RPT1.STORE_NBR == JNR_VENDOR_PROFILE.LOCATION_CODE],'inner')

# COMMAND ----------

# Processing node JNR_SKU_PROFILE_RPT, type JOINER 
# COLUMN COUNT: 26

JNR_SKU_PROFILE_RPT = SQ_Shortcut_to_SKU_PROFILE_RPT.join(JNR_SITE_PROFILE_RPT_1,[SQ_Shortcut_to_SKU_PROFILE_RPT.SKU_NBR == JNR_SITE_PROFILE_RPT_1.PRODUCT_CODE],'inner')

# COMMAND ----------

# Processing node JNR_SKU_PROFILE_RPT_1, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SKU_PROFILE_RPT1_temp = SQ_Shortcut_to_SKU_PROFILE_RPT1.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT1___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT1.columns])
JNR_SKU_PROFILE_RPT_temp = JNR_SKU_PROFILE_RPT.toDF(*["JNR_SKU_PROFILE_RPT___" + col for col in JNR_SKU_PROFILE_RPT.columns])

JNR_SKU_PROFILE_RPT_1 = SQ_Shortcut_to_SKU_PROFILE_RPT1_temp.join(JNR_SKU_PROFILE_RPT_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT1_temp.SQ_Shortcut_to_SKU_PROFILE_RPT1___SKU_NBR == JNR_SKU_PROFILE_RPT_temp.JNR_SKU_PROFILE_RPT___REPLACES_PRODUCT],'right_outer').selectExpr(
	"JNR_SKU_PROFILE_RPT___SUPPLIER_CODE as SUPPLIER_CODE",
	"JNR_SKU_PROFILE_RPT___PURCHASE_GROUP as PURCHASE_GROUP",
	"JNR_SKU_PROFILE_RPT___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"JNR_SKU_PROFILE_RPT___TERMINATION_DATE as TERMINATION_DATE",
	"JNR_SKU_PROFILE_RPT___REPLACES_PRODUCT as REPLACES_PRODUCT",
	"JNR_SKU_PROFILE_RPT___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"JNR_SKU_PROFILE_RPT___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"JNR_SKU_PROFILE_RPT___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"JNR_SKU_PROFILE_RPT___ORDER_MODEL as ORDER_MODEL",
	"JNR_SKU_PROFILE_RPT___FORECAST_MODEL as FORECAST_MODEL",
	"JNR_SKU_PROFILE_RPT___ABC_CLASS as ABC_CLASS",
	"JNR_SKU_PROFILE_RPT___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"JNR_SKU_PROFILE_RPT___XYZ_CLASS as XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"JNR_SKU_PROFILE_RPT___END_BALANCE as END_BALANCE",
	"JNR_SKU_PROFILE_RPT___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"JNR_SKU_PROFILE_RPT___MIN_FILL as MIN_FILL",
	"JNR_SKU_PROFILE_RPT___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"JNR_SKU_PROFILE_RPT___VENDOR_ID as VENDOR_ID",
	"JNR_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___PRODUCT_ID as OLD_PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT1___SKU_NBR as SKU_NBR",
	"JNR_SKU_PROFILE_RPT___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"JNR_SKU_PROFILE_RPT___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY")

# COMMAND ----------

# Processing node JNR_SKU_PROFILE_RPT_2, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_RPT_1_temp = JNR_SKU_PROFILE_RPT_1.toDF(*["JNR_SKU_PROFILE_RPT_1___" + col for col in JNR_SKU_PROFILE_RPT_1.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT2_temp = SQ_Shortcut_to_SKU_PROFILE_RPT2.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT2___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT2.columns])

JNR_SKU_PROFILE_RPT_2 = SQ_Shortcut_to_SKU_PROFILE_RPT2_temp.join(JNR_SKU_PROFILE_RPT_1_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT2_temp.SQ_Shortcut_to_SKU_PROFILE_RPT2___SKU_NBR == JNR_SKU_PROFILE_RPT_1_temp.JNR_SKU_PROFILE_RPT_1___REPLACING_PRODUCT],'right_outer').selectExpr(
	"JNR_SKU_PROFILE_RPT_1___SUPPLIER_CODE as SUPPLIER_CODE",
	"JNR_SKU_PROFILE_RPT_1___PURCHASE_GROUP as PURCHASE_GROUP",
	"JNR_SKU_PROFILE_RPT_1___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"JNR_SKU_PROFILE_RPT_1___TERMINATION_DATE as TERMINATION_DATE",
	"JNR_SKU_PROFILE_RPT_1___REPLACING_PRODUCT as REPLACING_PRODUCT",
	"JNR_SKU_PROFILE_RPT_1___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"JNR_SKU_PROFILE_RPT_1___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"JNR_SKU_PROFILE_RPT_1___ORDER_MODEL as ORDER_MODEL",
	"JNR_SKU_PROFILE_RPT_1___FORECAST_MODEL as FORECAST_MODEL",
	"JNR_SKU_PROFILE_RPT_1___ABC_CLASS as ABC_CLASS",
	"JNR_SKU_PROFILE_RPT_1___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"JNR_SKU_PROFILE_RPT_1___XYZ_CLASS as XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT_1___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT_1___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"JNR_SKU_PROFILE_RPT_1___END_BALANCE as END_BALANCE",
	"JNR_SKU_PROFILE_RPT_1___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"JNR_SKU_PROFILE_RPT_1___MIN_FILL as MIN_FILL",
	"JNR_SKU_PROFILE_RPT_1___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"JNR_SKU_PROFILE_RPT_1___VENDOR_ID as VENDOR_ID",
	"JNR_SKU_PROFILE_RPT_1___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT_1___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE_RPT_1___OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT2___PRODUCT_ID as NEW_PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT2___SKU_NBR as SKU_NBR",
	"JNR_SKU_PROFILE_RPT_1___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"JNR_SKU_PROFILE_RPT_1___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY")

# COMMAND ----------

# Processing node JNR_SKU_PROFILE_RPT_3, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SKU_PROFILE_RPT3_temp = SQ_Shortcut_to_SKU_PROFILE_RPT3.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT3___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT3.columns])
JNR_SKU_PROFILE_RPT_2_temp = JNR_SKU_PROFILE_RPT_2.toDF(*["JNR_SKU_PROFILE_RPT_2___" + col for col in JNR_SKU_PROFILE_RPT_2.columns])

JNR_SKU_PROFILE_RPT_3 = SQ_Shortcut_to_SKU_PROFILE_RPT3_temp.join(JNR_SKU_PROFILE_RPT_2_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT3_temp.SQ_Shortcut_to_SKU_PROFILE_RPT3___SKU_NBR == JNR_SKU_PROFILE_RPT_2_temp.JNR_SKU_PROFILE_RPT_2___REFERENCE_PRODUCT],'right_outer').selectExpr(
	"JNR_SKU_PROFILE_RPT_2___SUPPLIER_CODE as SUPPLIER_CODE",
	"JNR_SKU_PROFILE_RPT_2___PURCHASE_GROUP as PURCHASE_GROUP",
	"JNR_SKU_PROFILE_RPT_2___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"JNR_SKU_PROFILE_RPT_2___TERMINATION_DATE as TERMINATION_DATE",
	"JNR_SKU_PROFILE_RPT_2___REFERENCE_PRODUCT as REFERENCE_PRODUCT",
	"JNR_SKU_PROFILE_RPT_2___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"JNR_SKU_PROFILE_RPT_2___ORDER_MODEL as ORDER_MODEL",
	"JNR_SKU_PROFILE_RPT_2___FORECAST_MODEL as FORECAST_MODEL",
	"JNR_SKU_PROFILE_RPT_2___ABC_CLASS as ABC_CLASS",
	"JNR_SKU_PROFILE_RPT_2___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"JNR_SKU_PROFILE_RPT_2___XYZ_CLASS as XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT_2___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT_2___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"JNR_SKU_PROFILE_RPT_2___END_BALANCE as END_BALANCE",
	"JNR_SKU_PROFILE_RPT_2___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"JNR_SKU_PROFILE_RPT_2___MIN_FILL as MIN_FILL",
	"JNR_SKU_PROFILE_RPT_2___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"JNR_SKU_PROFILE_RPT_2___VENDOR_ID as VENDOR_ID",
	"JNR_SKU_PROFILE_RPT_2___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT_2___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE_RPT_2___OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT_2___NEW_PRODUCT_ID as NEW_PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT3___PRODUCT_ID as REFERENCE_PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT3___SKU_NBR as SKU_NBR",
	"JNR_SKU_PROFILE_RPT_2___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"JNR_SKU_PROFILE_RPT_2___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY")

# COMMAND ----------

# Processing node EXP_VENDOR_SUB_GROUP, type EXPRESSION 
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_RPT_3_temp = JNR_SKU_PROFILE_RPT_3.toDF(*["JNR_SKU_PROFILE_RPT_3___" + col for col in JNR_SKU_PROFILE_RPT_3.columns])

EXP_VENDOR_SUB_GROUP = JNR_SKU_PROFILE_RPT_3_temp.selectExpr(
	# "JNR_SKU_PROFILE_RPT_3___sys_row_id as sys_row_id",
	"CURRENT_TIMESTAMP as o_SNAPSHOT_DT",
	"JNR_SKU_PROFILE_RPT_3___PURCHASE_GROUP as PURCHASE_GROUP",
	"IF (INSTR ( JNR_SKU_PROFILE_RPT_3___SUPPLIER_CODE , '-' ) = 0, NULL, SUBSTR ( JNR_SKU_PROFILE_RPT_3___SUPPLIER_CODE , INSTR ( JNR_SKU_PROFILE_RPT_3___SUPPLIER_CODE , '-' ) + 1 , LENGTH(JNR_SKU_PROFILE_RPT_3___SUPPLIER_CODE) - INSTR ( JNR_SKU_PROFILE_RPT_3___SUPPLIER_CODE , '-' ) )) as o_SOURCE_VENDOR_SUB_GROUP",
	"JNR_SKU_PROFILE_RPT_3___INTRODUCTION_DATE as INTRODUCTION_DATE",
	"JNR_SKU_PROFILE_RPT_3___TERMINATION_DATE as TERMINATION_DATE",
	"JNR_SKU_PROFILE_RPT_3___REFERENCE_SCALING_FACTOR as REFERENCE_SCALING_FACTOR",
	"JNR_SKU_PROFILE_RPT_3___ORDER_MODEL as ORDER_MODEL",
	"JNR_SKU_PROFILE_RPT_3___FORECAST_MODEL as FORECAST_MODEL",
	"JNR_SKU_PROFILE_RPT_3___ABC_CLASS as ABC_CLASS",
	"JNR_SKU_PROFILE_RPT_3___PRODUCT_ABC_CLASS as PRODUCT_ABC_CLASS",
	"JNR_SKU_PROFILE_RPT_3___XYZ_CLASS as XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT_3___PRODUCT_XYZ_CLASS as PRODUCT_XYZ_CLASS",
	"JNR_SKU_PROFILE_RPT_3___DEMAND_SATISFIED_PCT as DEMAND_SATISFIED_PCT",
	"DECIMAL(JNR_SKU_PROFILE_RPT_3___END_BALANCE) as o_END_BALANCE",
	"JNR_SKU_PROFILE_RPT_3___ORDER_PARAMETER_QTY as ORDER_PARAMETER_QTY",
	"JNR_SKU_PROFILE_RPT_3___MIN_FILL as MIN_FILL",
	"JNR_SKU_PROFILE_RPT_3___ORDER_BATCH_SIZE as ORDER_BATCH_SIZE",
	"JNR_SKU_PROFILE_RPT_3___VENDOR_ID as VENDOR_ID",
	"JNR_SKU_PROFILE_RPT_3___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT_3___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE_RPT_3___OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT_3___NEW_PRODUCT_ID as NEW_PRODUCT_ID",
	"JNR_SKU_PROFILE_RPT_3___REFERENCE_PRODUCT_ID as REFERENCE_PRODUCT_ID",
	"CURRENT_TIMESTAMP as o_LOAD_TSTMP",
	"JNR_SKU_PROFILE_RPT_3___ECONOMIC_ORDER_QTY as ECONOMIC_ORDER_QTY",
	"JNR_SKU_PROFILE_RPT_3___MIN_DELIVERY_QTY as MIN_DELIVERY_QTY"
)

# COMMAND ----------

# Processing node Shortcut_to_DP_PRODUCT_LOCATION_SETTINGS_HIST, type TARGET 
# COLUMN COUNT: 26


Shortcut_to_DP_PRODUCT_LOCATION_SETTINGS_HIST = EXP_VENDOR_SUB_GROUP.selectExpr(
	"CAST(o_SNAPSHOT_DT AS DATE) as SNAPSHOT_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(VENDOR_ID AS INT) as SOURCE_VENDOR_ID",
	"CAST(o_SOURCE_VENDOR_SUB_GROUP AS STRING) as SOURCE_VENDOR_SUB_GROUP",
	"CAST(PURCHASE_GROUP AS STRING) as DP_PURCH_GROUP_ID",
	"CAST(INTRODUCTION_DATE AS DATE) as PRODUCT_DP_START_DT",
	"CAST(TERMINATION_DATE AS DATE) as PRODUCT_DP_END_DT",
	"CAST(OLD_PRODUCT_ID AS INT) as OLD_PRODUCT_ID",
	"CAST(NEW_PRODUCT_ID AS INT) as NEW_PRODUCT_ID",
	"CAST(REFERENCE_PRODUCT_ID AS INT) as REFERENCE_PRODUCT_ID",
	"CAST(REFERENCE_SCALING_FACTOR AS DECIMAL(8,4)) as REFERENCE_SCALING_FACTOR",
	"CAST(ORDER_MODEL AS STRING) as ORDER_MODEL",
	"CAST(FORECAST_MODEL AS STRING) as FORECAST_MODEL",
	"CAST(ABC_CLASS AS STRING) as PRODUCT_LOCATION_ABC_CLASS",
	"CAST(PRODUCT_ABC_CLASS AS STRING) as PRODUCT_ABC_CLASS",
	"CAST(XYZ_CLASS AS STRING) as PRODUCT_LOCATION_XYZ_CLASS",
	"CAST(PRODUCT_XYZ_CLASS AS STRING) as PRODUCT_XYZ_CLASS",
	"CAST(DEMAND_SATISFIED_PCT AS DECIMAL(8,4)) as DEMAND_SATISFIED_PCT",
	"CAST(o_END_BALANCE AS DECIMAL(12,4)) as END_BALANCE_QTY",
	"CAST(ORDER_PARAMETER_QTY AS DECIMAL(12,4)) as ORDER_PARAMETER_QTY",
	"CAST(MIN_FILL AS DECIMAL(12,4)) as MIN_FILL_QTY",
	"CAST(ORDER_BATCH_SIZE AS DECIMAL(12,4)) as ORDER_BATCH_SIZE",
	"CAST(ECONOMIC_ORDER_QTY AS DECIMAL(12,4)) as ECONOMIC_ORDER_QTY",
	"CAST(MIN_DELIVERY_QTY AS DECIMAL(12,4)) as MIN_DELIVERY_QTY",
	"CAST(o_LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_DP_PRODUCT_LOCATION_SETTINGS_HIST.write.saveAsTable(f'{legacy}.DP_PRODUCT_LOCATION_SETTINGS_HIST', mode = 'append')
logPrevRunDt("DP_PRODUCT_LOCATION_SETTINGS_HIST", "DP_PRODUCT_LOCATION_SETTINGS_HIST", "Completed", "N/A", f"{raw}.log_run_details")
