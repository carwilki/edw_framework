# Databricks notebook source
# Code converted on 2023-10-27 08:40:27
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
from Datalake.utils.pk import *

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

# Processing node SQ_Shortcut_to_CKB_PLANNER_DETAIL_HIST, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_CKB_PLANNER_DETAIL_HIST = spark.sql(f"""SELECT
CKB_PLANNER_DETAIL_HIST.SNAPSHOT_DT,
CKB_PLANNER_DETAIL_HIST.LOCATION_ID,
CKB_PLANNER_DETAIL_HIST.POG_DBKEY,
CKB_PLANNER_DETAIL_HIST.POG_VERSION_KEY,
CKB_PLANNER_DETAIL_HIST.PRODUCT_ID,
CKB_PLANNER_DETAIL_HIST.EFF_START_DT
FROM {legacy}.CKB_PLANNER_DETAIL_HIST
WHERE SNAPSHOT_DT < DATE(CURRENT_DATE - INTERVAL '85 days')""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_CKB_PLANNER_DETAIL_HIST1, type TARGET 
# COLUMN COUNT: 13


Shortcut_to_CKB_PLANNER_DETAIL_HIST1 = SQ_Shortcut_to_CKB_PLANNER_DETAIL_HIST.selectExpr(
	"CAST(SNAPSHOT_DT AS DATE) as SNAPSHOT_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_DBKEY AS INT) as POG_DBKEY",
	"CAST(POG_VERSION_KEY AS INT) as POG_VERSION_KEY",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(EFF_START_DT AS DATE) as EFF_START_DT"
)


Shortcut_to_CKB_PLANNER_DETAIL_HIST1.createOrReplaceTempView("Shortcut_to_CKB_PLANNER_DETAIL_HIST1")
spark.sql(f"""
MERGE INTO {legacy}.CKB_PLANNER_DETAIL_HIST target
USING Shortcut_to_CKB_PLANNER_DETAIL_HIST1 source
ON source.SNAPSHOT_DT = target.SNAPSHOT_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.POG_DBKEY = target.POG_DBKEY AND source.POG_VERSION_KEY = target.POG_VERSION_KEY AND source.PRODUCT_ID = target.PRODUCT_ID AND source.EFF_START_DT = target.EFF_START_DT
WHEN MATCHED THEN
  DELETE """
)

