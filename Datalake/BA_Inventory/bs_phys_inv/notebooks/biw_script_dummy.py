# Databricks notebook source
# Code converted on 2023-08-22 11:01:59
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

dbutils.widgets.text(name="path", defaultValue="/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test")
path = dbutils.widgets.get("path")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_DUMMY_SOURCE, type SOURCE 
# COLUMN COUNT: 2

SQ_DUMMY_SOURCE = spark.sql(f"""SELECT CURRENT_TIMESTAMP """).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_DUMMY_SOURCE = SQ_DUMMY_SOURCE \
	.withColumnRenamed(SQ_DUMMY_SOURCE.columns[0],'WEEK_DT')

# COMMAND ----------

# Processing node Shortcut_To_BIW_SCRIPT_DUMMY, type TARGET 
# COLUMN COUNT: 1


Shortcut_To_BIW_SCRIPT_DUMMY = SQ_DUMMY_SOURCE.selectExpr(
	"WEEK_DT as DATE_TIME"
)
Shortcut_To_BIW_SCRIPT_DUMMY.write.format('csv').option('header','false').mode('overwrite').option("sep",",").csv(path+'/biw_script_dummy.txt')

# COMMAND ----------



# COMMAND ----------

# Processing node Shortcut_To_BIW_SCRIPT_DUMMY, type TARGET 
# COLUMN COUNT: 1


Shortcut_To_BIW_SCRIPT_DUMMY = SQ_DUMMY_SOURCE.selectExpr(
	"WEEK_DT as DATE_TIME"
)
Shortcut_To_BIW_SCRIPT_DUMMY.write.format('csv').option('header','false').mode('overwrite').option("sep",",").csv(path+'/physinv_END.out')

# COMMAND ----------


