# Databricks notebook source
# Code converted on 2023-08-24 09:26:44
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

# Processing node SQ_Shortcut_to_SLOT_TREE_WEEK_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SLOT_TREE_WEEK_PRE = spark.sql(f"""select s.location_id, s.week_dt

  from {raw}.slot_tree_week_pre s

left outer join (select dc_location_id, week_dt from {legacy}.slot_tree_week group by 1,2) t

  on s.location_id = t.dc_location_id

  and s.week_dt = t.week_dt

where t.dc_location_id is not null

group by s.location_id, s.week_dt""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_TREE_WEEK_PRE = SQ_Shortcut_to_SLOT_TREE_WEEK_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns[1],'WEEK_DT')

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SLOT_TREE_WEEK_PRE_temp = SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.toDF(*["SQ_Shortcut_to_SLOT_TREE_WEEK_PRE___" + col for col in SQ_Shortcut_to_SLOT_TREE_WEEK_PRE.columns])

UPDTRANS = SQ_Shortcut_to_SLOT_TREE_WEEK_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SLOT_TREE_WEEK_PRE___LOCATION_ID as DC_LOCATION_ID",
	"SQ_Shortcut_to_SLOT_TREE_WEEK_PRE___WEEK_DT as WEEK_DT") \
	.withColumn('pyspark_data_action', lit(2))

# COMMAND ----------

# Processing node Shortcut_to_SLOT_TREE_WEEK, type TARGET 
# COLUMN COUNT: 52


Shortcut_to_SLOT_TREE_WEEK = UPDTRANS.selectExpr(
	"CAST(DC_LOCATION_ID AS INT) as DC_LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_TREE_WEEK,'DC_NBR',dcnbr,f'{raw}.SLOT_TREE_WEEK')
#Shortcut_to_SLOT_TREE_WEEK.write.mode("overwrite").saveAsTable(f'{refine}.SLOT_TREE_WEEK')
Shortcut_to_SLOT_TREE_WEEK.createOrReplaceTempView('Shortcut_to_SLOT_TREE_WEEK')

# COMMAND ----------

spark.sql(f"""
MERGE INTO {legacy}.SLOT_TREE_WEEK target
USING Shortcut_to_SLOT_TREE_WEEK source
ON source.DC_LOCATION_ID = target.DC_LOCATION_ID and source.WEEK_DT = target.WEEK_DT 
WHEN MATCHED THEN
  DELETE """
)


# COMMAND ----------


