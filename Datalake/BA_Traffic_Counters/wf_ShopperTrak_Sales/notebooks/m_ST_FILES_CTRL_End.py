# Databricks notebook source
#Code converted on 2023-11-29 14:36:05
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_ST_FILES_CTRL, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_ST_FILES_CTRL = spark.sql(f"""SELECT
ST_ID,
ST_CREATE_FILE_FLAG
FROM {legacy}.ST_FILES_CTRL WHERE ST_CREATE_FILE_FLAG ='Y'""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_ST_FILES_CTRL = SQ_Shortcut_to_ST_FILES_CTRL \
	.withColumnRenamed(SQ_Shortcut_to_ST_FILES_CTRL.columns[0],'ST_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ST_FILES_CTRL.columns[1],'ST_CREATE_FILE_FLAG')

# SQ_Shortcut_to_ST_FILES_CTRL = SQ_Shortcut_to_ST_FILES_CTRL.show()

# COMMAND ----------

# Processing node Shortcut_to_ST_FILES_CTRL1, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_ST_FILES_CTRL1 = SQ_Shortcut_to_ST_FILES_CTRL.selectExpr(
	"CAST(ST_ID AS INT) as ST_ID",
	"'N' as ST_CREATE_FILE_FLAG",
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP"
)

# Shortcut_to_ST_FILES_CTRL1.show()

# COMMAND ----------

Shortcut_to_ST_FILES_CTRL1.createOrReplaceTempView('ST_FILES_CTRL_UPD')

spark.sql(f"""
          MERGE INTO {legacy}.ST_FILES_CTRL trg
          USING ST_FILES_CTRL_UPD src
          ON (src.ST_ID =  trg.ST_ID  )
          WHEN MATCHED THEN UPDATE SET trg.ST_CREATE_FILE_FLAG = src.ST_CREATE_FILE_FLAG , trg.UPDATE_TSTMP = src.UPDATE_TSTMP 
          """)

# COMMAND ----------


