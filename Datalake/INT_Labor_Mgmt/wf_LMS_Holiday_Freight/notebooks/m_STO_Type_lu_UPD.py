# Databricks notebook source
# Code converted on 2023-09-05 14:08:56
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

# Processing node ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE, type SOURCE 
# COLUMN COUNT: 4

ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2 = spark.sql(f"""SELECT STO_TYPE_LU.STO_TYPE_ID,

       ZTB_EVENT_CTRL_PRE.STO_TYPE,

       ZTB_EVENT_CTRL_PRE.STR_BEG_DATE,

       ZTB_EVENT_CTRL_PRE.STR_END_DATE

  FROM {raw}.ZTB_EVENT_CTRL_PRE

          join

       {legacy}.STO_TYPE_LU

          ON (STO_TYPE_LU.sto_type = ZTB_EVENT_CTRL_PRE.sto_type)""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2 = ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2 \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2.columns[0],'STO_TYPE_ID') \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2.columns[1],'STO_TYPE') \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2.columns[2],'STR_BEG_DATE') \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2.columns[3],'STR_END_DATE')

# COMMAND ----------

# Processing node UPD_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 4

UPD_UPDATE = ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE2.selectExpr(
	"STO_TYPE_ID as STO_TYPE_ID",
	"STO_TYPE as STO_TYPE",
	"STR_BEG_DATE as STR_BEG_DATE",
	"STR_END_DATE as STR_END_DATE") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------

# Processing node Shortcut_to_STO_TYPE_LU_UPD, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_STO_TYPE_LU_UPD = UPD_UPDATE.selectExpr(
	"CAST(STO_TYPE_ID AS INT) as STO_TYPE_ID",
	"CAST(STO_TYPE AS STRING) as STO_TYPE",
	"CAST(STR_BEG_DATE AS TIMESTAMP) as STR_BEG_DATE",
	"CAST(STR_END_DATE AS TIMESTAMP) as STR_END_DATE"
)
#Shortcut_to_STO_TYPE_LU_UPD.write.mode("overwrite").saveAsTable(f'{legacy}.STO_TYPE_LU')

Shortcut_to_STO_TYPE_LU_UPD.createOrReplaceTempView("Shortcut_to_STO_TYPE_LU_UPD")
spark.sql(f"""
	MERGE INTO {legacy}.STO_TYPE_LU trg
    USING Shortcut_to_STO_TYPE_LU_UPD src
    ON src.STO_TYPE_ID = trg.STO_TYPE_ID
    WHEN MATCHED THEN UPDATE SET *
""")

# COMMAND ----------


