# Databricks notebook source
# Code converted on 2023-09-05 14:08:53
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
# COLUMN COUNT: 3

ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE = spark.sql(f"""SELECT ZTB_EVENT_CTRL_PRE.STO_TYPE,

       ZTB_EVENT_CTRL_PRE.STR_BEG_DATE,

       ZTB_EVENT_CTRL_PRE.STR_END_DATE

  FROM {raw}.ZTB_EVENT_CTRL_PRE

          left outer join

       {legacy}.STO_TYPE_LU

          ON (STO_TYPE_LU.sto_type = ZTB_EVENT_CTRL_PRE.sto_type)

 WHERE STO_TYPE_LU.STO_TYPE_ID IS NULL""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE = ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE.columns[0],'STO_TYPE') \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE.columns[1],'STR_BEG_DATE') \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE.columns[2],'STR_END_DATE') \
	.withColumnRenamed(ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE.columns[3],'STO_TYPE_ID')

# COMMAND ----------

# Processing node Shortcut_to_STO_TYPE_LU_INS, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_STO_TYPE_LU_INS = ASQ_Shortcut_to_ZTB_EVENT_CTRL_PRE.selectExpr(
	"CAST(STO_TYPE_ID AS INT) as STO_TYPE_ID",
	"CAST(STO_TYPE AS STRING) as STO_TYPE",
	"CAST(STR_BEG_DATE AS TIMESTAMP) as STR_BEG_DATE",
	"CAST(STR_END_DATE AS TIMESTAMP) as STR_END_DATE"
)
Shortcut_to_STO_TYPE_LU_INS.write.mode("append").saveAsTable(f'{legacy}.STO_TYPE_LU')


# COMMAND ----------


