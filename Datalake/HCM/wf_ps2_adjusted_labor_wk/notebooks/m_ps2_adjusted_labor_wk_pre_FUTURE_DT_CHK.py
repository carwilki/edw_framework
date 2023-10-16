# Databricks notebook source
# Code converted on 2023-09-06 09:51:46
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
empl_protected = getEnvPrefix(env) + 'empl_protected'

# COMMAND ----------

isvalidhour=dbutils.jobs.taskValues.get(taskKey = "m_ps2_adjusted_labor_wk_pre", key = "isvalidhour", default = 'false', debugValue = 'false')
tgtsuccessrows=dbutils.jobs.taskValues.get(taskKey = "m_ps2_adjusted_labor_wk_pre", key = "tgtsuccessrows", default = 'false', debugValue = 'false')


# COMMAND ----------

if (isvalidhour =='true' or tgtsuccessrows=='true'):
    dbutils.notebook.exit('Decision condition not satisfied, exiting the notebook process')

# COMMAND ----------

# Processing node SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE = spark.sql(f"""SELECT week_dt

FROM {raw}.PS2_ADJUSTED_LABOR_WK_PRE

WHERE WEEK_DT > DECODE(DATE_PART('DOW',CURRENT_DATE), 1, CURRENT_DATE, (CURRENT_DATE - (DATE_PART('DOW',CURRENT_DATE)-1) +7 ))""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE = SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[0],'') \
	.withColumnRenamed(SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.columns[1],'WEEK_DT')

# COMMAND ----------

# # Processing node Shortcut_to_DUMMY_TARGET, type TARGET 
# # COLUMN COUNT: 1


# Shortcut_to_DUMMY_TARGET = SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.selectExpr(
# 	"CAST(WEEK_DT AS STRING) as COMMENT"
# )
# Shortcut_to_DUMMY_TARGET.write.mode("append").saveAsTable(f'{legacy}.DUMMY_TARGET')

# COMMAND ----------

if SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_PRE.count() > 0:
    dbutils.jobs.taskValues.set(key = "tgtsuccessrowsdummy", value = 'true')
