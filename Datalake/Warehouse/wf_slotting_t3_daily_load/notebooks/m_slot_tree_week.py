# Databricks notebook source
# Code converted on 2023-08-24 09:26:39
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

# Processing node SQ_Shortcut_to_SLOT_TREE_WEEK_PRE, type SOURCE 
# COLUMN COUNT: 52

SQ_Shortcut_to_SLOT_TREE_WEEK_PRE = spark.sql(f"""SELECT  LOCATION_ID,

        NODE_ID,

        SLOT_ID,

SLOT_HT,

SLOT_WIDTH, SLOT_DEPTH,

WT_LIMIT,RACK_TYPE,

RACK_TYPE_NAME,

        WEEK_DT,

        DAY_OF_WK_NBR,

        LVL,

        TREE_ORDER,

        rtrim(NODE_NAME) node_name,

        NODE1,

        NODE2,

        NODE3,

        NODE4,

        NODE5,

        NODE6,

        PRODUCT_ID,

        SKU_NBR,

        SKU_DESC,

        PALLET_HI,

        PALLET_TI,

        CASE_HT,

        CASE_LEN,

        CASE_WID,

        CASE_WT,

        EACH_HT,

        EACH_LEN,

        EACH_WID,

        EACH_WT,

        SLOTITEM_ID,

        CONST_GRP_ID,

        SL_CONST_GRP_DESC,

        GRP_GRP_ID,

        SEQ_GRP_ID,

        SCORE,

        LEGAL_FIT,

        LEGAL_FIT_REASON,

        COLOR_ID,

        COLOR_DESC,

        GROUP_CD,

        group_desc,

        PKG_CD,

        pkg_desc,

        STATUS_CD,

        STATUS_DESC,

        trim(DSP_SLOT) dsp_slot,

        current_date update_dt,

        current_date load_dt

FROM {raw}.slot_tree_week_pre""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_TREE_WEEK_PRE = SQ_Shortcut_to_SLOT_TREE_WEEK_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[1],'NODE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[2],'SLOT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[3],'SLOT_HT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[4],'SLOT_WIDTH') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[5],'SLOT_DEPTH') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[6],'WT_LIMIT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[7],'RACK_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[8],'RACK_TYPE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[9],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[10],'DAY_OF_WK_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[11],'LVL') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[12],'TREE_ORDER') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[13],'NODE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[14],'NODE1') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[15],'NODE2') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[16],'NODE3') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[17],'NODE4') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[18],'NODE5') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[19],'NODE6') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[20],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[21],'SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[22],'SKU_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[23],'PALLET_HI') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[24],'PALLET_TI') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[25],'CASE_HT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[26],'CASE_LEN') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[27],'CASE_WID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[28],'CASE_WT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[29],'EACH_HT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[30],'EACH_LEN') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[31],'EACH_WID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[32],'EACH_WT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[33],'SLOTITEM_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[34],'CONST_GRP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[35],'SL_CONST_GRP_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[36],'GRP_GRP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[37],'SEQ_GRP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[38],'SCORE') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[39],'LEGAL_FIT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[40],'LEGAL_FIT_REASON') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[41],'COLOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[42],'COLOR_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[43],'GROUP_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[44],'GROUP_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[45],'PKG_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[46],'PKG_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[47],'STATUS_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[48],'STATUS_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[49],'DSP_SLOT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[50],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[51],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_SLOT_TREE_WEEK1, type TARGET 
# COLUMN COUNT: 52


Shortcut_to_SLOT_TREE_WEEK1 = SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.selectExpr(
	"CAST(LOCATION_ID AS INT) as DC_LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(NODE_ID AS INT) as SL_NODE_ID",
	"CAST(SLOT_ID AS INT) as SL_SLOT_ID",
	"CAST(SLOT_HT AS DECIMAL(9,4)) as SL_SLOT_HT",
	"CAST(SLOT_WIDTH AS DECIMAL(9,4)) as SL_SLOT_WIDTH",
	"CAST(SLOT_DEPTH AS DECIMAL(9,4)) as SL_SLOT_DEPTH",
	"CAST(WT_LIMIT AS DECIMAL(13,4)) as SL_WT_LIMIT",
	"CAST(RACK_TYPE AS INT) as SL_RACK_TYPE",
	"CAST(RACK_TYPE_NAME AS STRING) as SL_RACK_TYPE_NAME",
	"DAY_OF_WK_NBR as DAY_OF_WK_NBR",
	"LVL as SL_LVL",
	"CAST(TREE_ORDER AS INT) as SL_TREE_ORDER",
	"CAST(NODE_NAME AS STRING) as SL_NODE_NAME",
	"CAST(NODE1 AS STRING) as SL_NODE1",
	"CAST(NODE2 AS STRING) as SL_NODE2",
	"CAST(NODE3 AS STRING) as SL_NODE3",
	"CAST(NODE4 AS STRING) as SL_NODE4",
	"CAST(NODE5 AS STRING) as SL_NODE5",
	"CAST(NODE6 AS STRING) as SL_NODE6",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(SKU_NBR AS INT) as SL_SKU_NBR",
	"CAST(SKU_DESC AS STRING) as SL_SKU_DESC",
	"CAST(PALLET_HI AS INT) as SL_PALLET_HI",
	"CAST(PALLET_TI AS INT) as SL_PALLET_TI",
	"CAST(CASE_HT AS DECIMAL(9,4)) as SL_CASE_HT",
	"CAST(CASE_LEN AS DECIMAL(9,4)) as SL_CASE_LEN",
	"CAST(CASE_WID AS DECIMAL(9,4)) as SL_CASE_WID",
	"CAST(CASE_WT AS DECIMAL(9,4)) as SL_CASE_WT",
	"CAST(EACH_HT AS DECIMAL(9,4)) as SL_EACH_HT",
	"CAST(EACH_LEN AS DECIMAL(9,4)) as SL_EACH_LEN",
	"CAST(EACH_WID AS DECIMAL(9,4)) as SL_EACH_WID",
	"CAST(EACH_WT AS DECIMAL(9,4)) as SL_EACH_WT",
	"CAST(SLOTITEM_ID AS INT) as SL_SLOTITEM_ID",
	"CAST(CONST_GRP_ID AS INT) as SL_CONST_GRP_ID",
	"CAST(SL_CONST_GRP_DESC AS STRING) as SL_CONST_GRP_DESC",
	"CAST(GRP_GRP_ID AS INT) as SL_GRP_GRP_ID",
	"CAST(SEQ_GRP_ID AS INT) as SL_SEQ_GRP_ID",
	"CAST(SCORE AS DECIMAL(9,4)) as SL_SCORE",
	"LEGAL_FIT as SL_LEGAL_FIT",
	"CAST(LEGAL_FIT_REASON AS INT) as SL_LEGAL_FIT_REASON",
	"COLOR_ID as SL_COLOR_ID",
	"CAST(COLOR_DESC AS STRING) as SL_COLOR_DESC",
	"CAST(GROUP_CD AS STRING) as SL_GROUP_CD",
	"CAST(GROUP_DESC AS STRING) as SL_GROUP_DESC",
	"CAST(PKG_CD AS STRING) as SL_PKG_TYPE_CD",
	"CAST(PKG_DESC AS STRING) as SL_PKG_DESC",
	"CAST(STATUS_CD AS STRING) as SL_STATUS_CD",
	"CAST(STATUS_DESC AS STRING) as SL_STATUS_DESC",
	"CAST(DSP_SLOT AS STRING) as SL_DSP_SLOT",
	"CAST(UPDATE_DT AS DATE) as UPDATE_DT",
	"CAST(LOAD_DT AS DATE) as LOAD_DT"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_TREE_WEEK1,'DC_NBR',dcnbr,f'{raw}.SLOT_TREE_WEEK')
Shortcut_to_SLOT_TREE_WEEK1.write.mode("append").saveAsTable(f'{legacy}.SLOT_TREE_WEEK')
