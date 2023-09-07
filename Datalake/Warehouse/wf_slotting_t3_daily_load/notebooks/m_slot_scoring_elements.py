# Databricks notebook source
# Code converted on 2023-08-24 09:26:52
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
# COLUMN COUNT: 7

SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE = spark.sql(f"""SELECT  COLOR_CODE_NBR,

GROUP_NBR, COLOR_CODE_DESC, SLOT_COLOR,

nvl(GROUP_MIN,0) gmin, NVL(GROUP_MAX,0) gmax, CURRENT_DATE

FROM {raw}.SLOT_SCORING_ELEMENTS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE = SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[0],'COLOR_CODE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[1],'GROUP_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[2],'COLOR_CODE_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[3],'SLOT_COLOR') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[4],'GROUP_MIN') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[5],'GROUP_MAX') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns[6],'LOAD_DT')

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE_temp = SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.toDF(*["SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___" + col for col in SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE.columns])

UPDTRANS = SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___COLOR_CODE_NBR as COLOR_CODE_NBR",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___GROUP_NBR as GROUP_NBR",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___COLOR_CODE_DESC as COLOR_CODE_DESC",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___SLOT_COLOR as SLOT_COLOR",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___GROUP_MIN as GROUP_MIN",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___GROUP_MAX as GROUP_MAX",
	"SQ_Shortcut_to_SLOT_SCORING_ELEMENTS_PRE___LOAD_DT as LOAD_DT") \
	.withColumn('pyspark_data_action', lit(0))

# COMMAND ----------

# Processing node Shortcut_to_SLOT_SCORING_ELEMENTS1, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_SLOT_SCORING_ELEMENTS1 = UPDTRANS.selectExpr(
	"CAST(COLOR_CODE_NBR AS INT) as SL_ELEMENT_ID",
	"CAST(GROUP_NBR AS INT) as SL_COLOR_ID",
	"CAST(COLOR_CODE_DESC AS STRING) as SL_ELEMENT_DESC",
	"CAST(SLOT_COLOR AS STRING) as SL_COLOR_DESC",
	"CAST(GROUP_MIN AS DECIMAL(9,2)) as SL_GROUP_MIN",
	"CAST(GROUP_MAX AS DECIMAL(9,2)) as SL_GROUP_MAX",
	"CAST(LOAD_DT AS DATE) as UPDATE_DT",
	"CAST(LOAD_DT AS DATE) as LOAD_DT"#,
	# "pyspark_data_action as pyspark_data_action"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_SCORING_ELEMENTS1,'DC_NBR',dcnbr,f'{raw}.SLOT_SCORING_ELEMENTS'
Shortcut_to_SLOT_SCORING_ELEMENTS1.write.mode("append").saveAsTable(f'{legacy}.SLOT_SCORING_ELEMENTS')

