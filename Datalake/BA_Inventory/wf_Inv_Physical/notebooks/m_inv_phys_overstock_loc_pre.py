# Databricks notebook source
#Code converted on 2023-09-26 09:20:12
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

# Processing node SQ_Shortcut_to_SAP_IKPF_PRE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_SAP_IKPF_PRE = spark.sql(f"""SELECT
MANDT,
DOC_NBR,
FISCAL_YR,
SITE_NBR,
GIDAT,
POSTING_DT
FROM {raw}.SAP_IKPF_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_RF_PHYINV_PRE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_ZTB_RF_PHYINV_PRE = spark.sql(f"""SELECT
MANDT,
SITE,
LOCATION,
ARTICLE,
COUNT_QTY,
POST_DATE
FROM {raw}.ZTB_RF_PHYINV_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_ISEG_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_SAP_ISEG_PRE = spark.sql(f"""SELECT
MANDT,
DOC_NBR,
FISCAL_YR,
SKU_NBR
FROM {raw}.SAP_ISEG_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE = spark.sql(f"""SELECT
  PRODUCT_ID,
  SKU_NBR
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE = spark.sql(f"""SELECT
OVERSTOCK_LOC_TYPE_EFF_DT,
OVERSTOCK_LOC_TYPE_ID,
OVERSTOCK_LOC_TYPE_END_DT,
LIMIT_LOW_END,
LIMIT_HIGH_END
FROM {legacy}.INV_PHYS_OVERSTOCK_LOC_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SAP_ISEG_PRE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_IKPF_PRE_temp = SQ_Shortcut_to_SAP_IKPF_PRE.toDF(*["SQ_Shortcut_to_SAP_IKPF_PRE___" + col for col in SQ_Shortcut_to_SAP_IKPF_PRE.columns])
SQ_Shortcut_to_SAP_ISEG_PRE_temp = SQ_Shortcut_to_SAP_ISEG_PRE.toDF(*["SQ_Shortcut_to_SAP_ISEG_PRE___" + col for col in SQ_Shortcut_to_SAP_ISEG_PRE.columns])

JNR_SAP_ISEG_PRE = SQ_Shortcut_to_SAP_IKPF_PRE_temp.join(SQ_Shortcut_to_SAP_ISEG_PRE_temp,[SQ_Shortcut_to_SAP_IKPF_PRE_temp.SQ_Shortcut_to_SAP_IKPF_PRE___MANDT == SQ_Shortcut_to_SAP_ISEG_PRE_temp.SQ_Shortcut_to_SAP_ISEG_PRE___MANDT, SQ_Shortcut_to_SAP_IKPF_PRE_temp.SQ_Shortcut_to_SAP_IKPF_PRE___DOC_NBR == SQ_Shortcut_to_SAP_ISEG_PRE_temp.SQ_Shortcut_to_SAP_ISEG_PRE___DOC_NBR, SQ_Shortcut_to_SAP_IKPF_PRE_temp.SQ_Shortcut_to_SAP_IKPF_PRE___FISCAL_YR == SQ_Shortcut_to_SAP_ISEG_PRE_temp.SQ_Shortcut_to_SAP_ISEG_PRE___FISCAL_YR],'inner').selectExpr(
	"SQ_Shortcut_to_SAP_ISEG_PRE___MANDT as dtl_MANDT",
	"SQ_Shortcut_to_SAP_ISEG_PRE___DOC_NBR as dtl_DOC_NBR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___FISCAL_YR as dtl_FISCAL_YR",
	"SQ_Shortcut_to_SAP_ISEG_PRE___SKU_NBR as dtl_SKU_NBR",
	"SQ_Shortcut_to_SAP_IKPF_PRE___MANDT as MANDT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___DOC_NBR as DOC_NBR",
	"SQ_Shortcut_to_SAP_IKPF_PRE___FISCAL_YR as FISCAL_YR",
	"SQ_Shortcut_to_SAP_IKPF_PRE___SITE_NBR as SITE_NBR",
	"SQ_Shortcut_to_SAP_IKPF_PRE___GIDAT as PLANNED_DT",
	"SQ_Shortcut_to_SAP_IKPF_PRE___POSTING_DT as POSTING_DT")

# COMMAND ----------

# Processing node EXP_ZTB_TABLE, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_RF_PHYINV_PRE_temp = SQ_Shortcut_to_ZTB_RF_PHYINV_PRE.toDF(*["SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___" + col for col in SQ_Shortcut_to_ZTB_RF_PHYINV_PRE.columns])

EXP_ZTB_TABLE = SQ_Shortcut_to_ZTB_RF_PHYINV_PRE_temp.selectExpr(
	# "SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___MANDT as MANDT",
	# "SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___SITE as SITE",
	# "SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___ARTICLE as ARTICLE",
	# "SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___LOCATION as LOCATION",
	# "SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___COUNT_QTY as COUNT_QTY",
	# "SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___POST_DATE as in_POST_DATE").selectExpr(
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___MANDT as MANDT",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___SITE as SITE",
	"cast(SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___SITE as int) as SITE_IN_NBR",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___ARTICLE as ARTICLE",
	"cast(SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___ARTICLE as int) as ARTICLE_IN_NBR",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___LOCATION as LOCATION",
	"SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___COUNT_QTY as COUNT_QTY",
	"DATE_ADD(SQ_Shortcut_to_ZTB_RF_PHYINV_PRE___POST_DATE, 1) as PLANNED_DT_ZTB",
	"1 as DUMMY_JOIN_CONDTION"
)

# COMMAND ----------

# Processing node EXP_DUMMY_JOIN_CONDITION, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE_temp = SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE.toDF(*["SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___" + col for col in SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE.columns])

EXP_DUMMY_JOIN_CONDITION = SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE_temp.selectExpr(
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___OVERSTOCK_LOC_TYPE_EFF_DT as OVERSTOCK_LOC_TYPE_EFF_DT",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___OVERSTOCK_LOC_TYPE_END_DT as OVERSTOCK_LOC_TYPE_END_DT",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___OVERSTOCK_LOC_TYPE_ID as OVERSTOCK_LOC_TYPE_ID",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___LIMIT_LOW_END as LIMIT_LOW_END",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_TYPE___LIMIT_HIGH_END as LIMIT_HIGH_END",
	"1 as DUMMY_JOIN_CONDITION"
)

# COMMAND ----------

# Processing node JNR_ZTB_RF_PHYINV_PRE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_SAP_ISEG_PRE_temp = JNR_SAP_ISEG_PRE.toDF(*["JNR_SAP_ISEG_PRE___" + col for col in JNR_SAP_ISEG_PRE.columns])
EXP_ZTB_TABLE_temp = EXP_ZTB_TABLE.toDF(*["EXP_ZTB_TABLE___" + col for col in EXP_ZTB_TABLE.columns])

JNR_ZTB_RF_PHYINV_PRE = EXP_ZTB_TABLE_temp.join(JNR_SAP_ISEG_PRE_temp,[EXP_ZTB_TABLE_temp.EXP_ZTB_TABLE___MANDT == JNR_SAP_ISEG_PRE_temp.JNR_SAP_ISEG_PRE___MANDT, EXP_ZTB_TABLE_temp.EXP_ZTB_TABLE___SITE == JNR_SAP_ISEG_PRE_temp.JNR_SAP_ISEG_PRE___SITE_NBR, EXP_ZTB_TABLE_temp.EXP_ZTB_TABLE___ARTICLE == JNR_SAP_ISEG_PRE_temp.JNR_SAP_ISEG_PRE___dtl_SKU_NBR, EXP_ZTB_TABLE_temp.EXP_ZTB_TABLE___PLANNED_DT_ZTB == JNR_SAP_ISEG_PRE_temp.JNR_SAP_ISEG_PRE___PLANNED_DT],'inner').selectExpr(
	"JNR_SAP_ISEG_PRE___MANDT as MANDT",
	"JNR_SAP_ISEG_PRE___FISCAL_YR as FISCAL_YR",
	"JNR_SAP_ISEG_PRE___PLANNED_DT as PLANNED_DT",
	"JNR_SAP_ISEG_PRE___POSTING_DT as POSTING_DT",
	"JNR_SAP_ISEG_PRE___SITE_NBR as SITE_NBR",
	"JNR_SAP_ISEG_PRE___dtl_SKU_NBR as dtl_SKU_NBR",
	"EXP_ZTB_TABLE___MANDT as MANDT1",
	"EXP_ZTB_TABLE___SITE as SITE",
	"EXP_ZTB_TABLE___SITE_IN_NBR as SITE_IN_NBR",
	"EXP_ZTB_TABLE___ARTICLE as ARTICLE",
	"EXP_ZTB_TABLE___ARTICLE_IN_NBR as ARTICLE_IN_NBR",
	"EXP_ZTB_TABLE___LOCATION as LOCATION",
	"EXP_ZTB_TABLE___COUNT_QTY as COUNT_QTY",
	"EXP_ZTB_TABLE___PLANNED_DT_ZTB as PLANNED_DT_ZTB",
	"EXP_ZTB_TABLE___DUMMY_JOIN_CONDTION as DUMMY_JOIN_CONDTION")

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
  LOCATION_ID,
  STORE_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER 
# COLUMN COUNT: 9

JNR_SKU_PROFILE = SQ_Shortcut_to_SKU_PROFILE.join(JNR_ZTB_RF_PHYINV_PRE,[SQ_Shortcut_to_SKU_PROFILE.SKU_NBR == JNR_ZTB_RF_PHYINV_PRE.ARTICLE_IN_NBR],'inner')

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 9

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(JNR_SKU_PROFILE,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == JNR_SKU_PROFILE.SITE_IN_NBR],'inner')

# COMMAND ----------

# Processing node JNR_INV_PHYS_OVERSTOCK_LOC_TYPE, type JOINER 
# COLUMN COUNT: 13

JNR_INV_PHYS_OVERSTOCK_LOC_TYPE = EXP_DUMMY_JOIN_CONDITION.join(JNR_SITE_PROFILE,[EXP_DUMMY_JOIN_CONDITION.DUMMY_JOIN_CONDITION == JNR_SITE_PROFILE.DUMMY_JOIN_CONDTION],'inner')

# COMMAND ----------

# Processing node FIL_ONLY_CORRECT_TYPES, type FILTER 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_INV_PHYS_OVERSTOCK_LOC_TYPE_temp = JNR_INV_PHYS_OVERSTOCK_LOC_TYPE.toDF(*["JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___" + col for col in JNR_INV_PHYS_OVERSTOCK_LOC_TYPE.columns])

FIL_ONLY_CORRECT_TYPES = JNR_INV_PHYS_OVERSTOCK_LOC_TYPE_temp.selectExpr(
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___POSTING_DT as POSTING_DT",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___LOCATION_ID as LOCATION_ID",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___PRODUCT_ID as PRODUCT_ID",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___LOCATION as LOCATION",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___PLANNED_DT_ZTB as PLANNED_DT_ZTB",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___COUNT_QTY as COUNT_QTY",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___OVERSTOCK_LOC_TYPE_EFF_DT as OVERSTOCK_LOC_TYPE_EFF_DT",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___OVERSTOCK_LOC_TYPE_END_DT as OVERSTOCK_LOC_TYPE_END_DT",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___OVERSTOCK_LOC_TYPE_ID as OVERSTOCK_LOC_TYPE_ID",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___LIMIT_LOW_END as LIMIT_LOW_END",
	"JNR_INV_PHYS_OVERSTOCK_LOC_TYPE___LIMIT_HIGH_END as LIMIT_HIGH_END").filter("POSTING_DT >= OVERSTOCK_LOC_TYPE_EFF_DT AND POSTING_DT <= OVERSTOCK_LOC_TYPE_END_DT AND LOCATION >= LIMIT_LOW_END AND LOCATION <= LIMIT_HIGH_END").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node AGG_TOTAL_COUNT_QTY, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

AGG_TOTAL_COUNT_QTY = FIL_ONLY_CORRECT_TYPES.selectExpr(
	"POSTING_DT as POSTING_DT",
	"LOCATION_ID as LOCATION_ID",
	"PRODUCT_ID as PRODUCT_ID",
	"LOCATION as LOCATION",
	"OVERSTOCK_LOC_TYPE_ID as in_OVERSTOCK_LOC_TYPE_ID",
	"PLANNED_DT_ZTB as in_PLANNED_DT_ZTB",
	"COUNT_QTY as in_COUNT_QTY") \
	.groupBy("POSTING_DT","LOCATION_ID","PRODUCT_ID","LOCATION") \
	.agg( 
        max(col('in_OVERSTOCK_LOC_TYPE_ID')).alias("OVERSTOCK_LOC_TYPE_ID"),
        max(col('in_PLANNED_DT_ZTB')).alias("PLANNED_DT_ZTB"),
        sum(col('in_COUNT_QTY')).alias("COUNT_QTY")
	).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE = AGG_TOTAL_COUNT_QTY.selectExpr(
	"CAST(POSTING_DT AS DATE) as POSTING_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION AS INT) as OVERSTOCK_LOC_NBR",
	"CAST(OVERSTOCK_LOC_TYPE_ID AS SMALLINT) as OVERSTOCK_LOC_TYPE_ID",
	"CAST(PLANNED_DT_ZTB AS DATE) as PLANNED_DT",
	"CAST(COUNT_QTY AS INT) as COUNT_QTY"
)

Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE.write.mode("overwrite").saveAsTable(f'{raw}.INV_PHYS_OVERSTOCK_LOC_PRE')
