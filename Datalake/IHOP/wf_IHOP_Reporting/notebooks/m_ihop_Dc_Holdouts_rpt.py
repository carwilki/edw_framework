# Databricks notebook source
# Code converted on 2023-08-24 13:54:02
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

# Processing node SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE = spark.sql(f"""SELECT IDH.DC_HOLDOUT_ID, sp1.location_id FROM_LOCATION_ID, IDH.DC_ID AS DC_NBR, sp.product_id, IDH.SKU_NBR, IDH.EVENT_ID,

IDH.REFERENCE_CD, IDH.HOLD_QTY, IDH.BEGIN_DATE, IDH.END_DATE, IDH.FLAGGED_FOR_REMOVAL,

IDH.LAST_ISSUED_ON, IDH.USER_KEY, IDH.IS_PENDING, IDH.MODIFIEDBY, IDH.MODIFIEDON, IDH.INSERT_DT

FROM {raw}.IHOP_DC_HOLDOUTS_RPT_PRE IDH

 JOIN {legacy}.SITE_PROFILE sp1

   on IDH.DC_ID = sp1.STORE_NBR

 JOIN {legacy}.SKU_PROFILE sp

   on IDH.SKU_NBR = sp.SKU_NBR""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE = SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[0],'DC_HOLDOUT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[1],'FROM_LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[2],'DC_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[3],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[4],'SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[5],'EVENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[6],'REFERENCE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[7],'HOLD_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[8],'BEGIN_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[9],'END_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[10],'FLAGGED_FOR_REMOVAL') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[11],'LAST_ISSUED_ON') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[12],'USER_KEY') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[13],'IS_PENDING') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[14],'MODIFIEDBY') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[15],'MODIFIEDON') \
	.withColumnRenamed(SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.columns[16],'INSERT_DT')

# COMMAND ----------

# Processing node Shortcut_to_IHOP_DC_HOLDOUTS_RPT, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_IHOP_DC_HOLDOUTS_RPT = SQ_Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.selectExpr(
	"CAST(DC_HOLDOUT_ID AS INT) as DC_HOLDOUT_ID",
	"CAST(FROM_LOCATION_ID AS INT) as FROM_LOCATION_ID",
	"CAST(DC_NBR AS INT) as DC_NBR",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(EVENT_ID AS INT) as EVENT_ID",
	"CAST(REFERENCE_CD AS STRING) as REFERENCE_CD",
	"CAST(HOLD_QTY AS INT) as HOLD_QTY",
	"CAST(BEGIN_DATE AS TIMESTAMP) as BEGIN_DATE",
	"CAST(END_DATE AS TIMESTAMP) as END_DATE",
	"FLAGGED_FOR_REMOVAL as FLAGGED_FOR_REMOVAL",
	"CAST(LAST_ISSUED_ON AS TIMESTAMP) as LAST_ISSUED_ON",
	"CAST(USER_KEY AS STRING) as USER_KEY",
	"IS_PENDING as IS_PENDING",
	"CAST(MODIFIEDBY AS STRING) as MODIFIEDBY",
	"CAST(MODIFIEDON AS TIMESTAMP) as MODIFIEDON",
	"CAST(INSERT_DT AS TIMESTAMP) as INSERT_DT"
)
# overwriteDeltaPartition(Shortcut_to_IHOP_DC_HOLDOUTS_RPT,'DC_NBR',dcnbr,f'{raw}.IHOP_DC_HOLDOUTS_RPT')
Shortcut_to_IHOP_DC_HOLDOUTS_RPT.write.mode("overwrite").saveAsTable(f'{legacy}.IHOP_DC_HOLDOUTS_RPT')

# COMMAND ----------


