# Databricks notebook source
# Code converted on 2023-10-24 09:48:29
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
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node SQ_Shortcut_To_DAYS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_DAYS = spark.sql(f"""SELECT
DAYS.DAY_DT
FROM {enterprise}.DAYS
WHERE DAYS.FISCAL_DAY_OF_MO_NBR IN (1,2,3,4,5,6,7)

AND

DAYS.DAY_DT = CURRENT_DATE-1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_DECISION, type EXPRESSION 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_DAYS_temp = SQ_Shortcut_To_DAYS.toDF(*["SQ_Shortcut_To_DAYS___" + col for col in SQ_Shortcut_To_DAYS.columns])

EXP_DECISION = SQ_Shortcut_To_DAYS_temp.selectExpr(
	"SQ_Shortcut_To_DAYS___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_DAYS___DAY_DT as DAY_DT"
)

# COMMAND ----------

# Processing node DATE_TYPE_DECISION, type TARGET 
# COLUMN COUNT: 1


DATE_TYPE_DECISION = EXP_DECISION.selectExpr(
	"DAY_DT as DATE_DT"
)
DATE_TYPE_DECISION.write.mode("overwrite").saveAsTable(f'{legacy}.DATE_TYPE_DECISION')

# COMMAND ----------

count=DATE_TYPE_DECISION.count()
print('The count of the pre table is ' + str(count))
dbutils.jobs.taskValues.set(key = "tgtsuccessrows", value = count)
