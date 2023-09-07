# Databricks notebook source
# Code converted on 2023-08-24 09:26:49
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

# Processing node SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE = spark.sql(f"""SELECT COLOR_CODE_NBR, GROUP_NBR
FROM {raw}.SLOT_SCORING_ELEMENTS_PRE
GROUP BY COLOR_CODE_NBR, GROUP_NBR""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE = SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[0],'COLOR_CODE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[1],'GROUP_NBR')

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE_temp = SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.toDF(*["SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___" + col for col in SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns])

UPDTRANS = SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___COLOR_CODE_NBR as COLOR_CODE_NBR",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___GROUP_NBR as GROUP_NBR") \
	.withColumn('pyspark_data_action', lit(2))

# COMMAND ----------

# Processing node Shortcut_to_SLOT_SCORING_ELEMENTS, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_SLOT_SCORING_ELEMENTS = UPDTRANS.selectExpr(
	"CAST(COLOR_CODE_NBR AS INT) as SL_ELEMENT_ID",
	"CAST(GROUP_NBR AS INT) as SL_COLOR_ID",
)

Shortcut_to_SLOT_SCORING_ELEMENTS.createOrReplaceTempView("Shortcut_to_SLOT_SCORING_ELEMENTS")
# overwriteDeltaPartition(Shortcut_to_SLOT_SCORING_ELEMENTS,'DC_NBR',dcnbr,f'{raw}.SLOT_SCORING_ELEMENTS')
#Shortcut_to_SLOT_SCORING_ELEMENTS.write.mode("overwrite").saveAsTable(f'{refine}.SLOT_SCORING_ELEMENTS')

# COMMAND ----------

spark.sql(f"""
MERGE INTO {legacy}.SLOT_SCORING_ELEMENTS target
USING Shortcut_to_SLOT_SCORING_ELEMENTS source
ON source.SL_ELEMENT_ID = target.SL_ELEMENT_ID and source.SL_COLOR_ID = target.SL_COLOR_ID 
WHEN MATCHED THEN
  DELETE """
)

