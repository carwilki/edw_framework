# Databricks notebook source
# Code converted on 2023-08-24 09:26:40
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

# Processing node SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE = spark.sql(f"""SELECT s.LOCATION_ID, s.WEEK_DT

FROM {raw}.SLOT_VIOLATION_DETAIL_PRE S

left outer join (select dc_location_id, week_dt from {legacy}.slot_violation_detail

               ) T

  ON s.location_id = t.dc_location_id

  and s.week_dt = t.week_dt

where t.dc_location_id is not null

""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE = SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns[1],'WEEK_DT')

# COMMAND ----------

# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE_temp = SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.toDF(*["SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE___" + col for col in SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE.columns])

UPDTRANS = SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SLOT_VIOLATION_DETAIL_PRE___WEEK_DT as WEEK_DT") \
	.withColumn('pyspark_data_action', lit(2))

# COMMAND ----------

# Processing node Shortcut_to_SLOT_VIOLATION_DETAIL, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_SLOT_VIOLATION_DETAIL = UPDTRANS.selectExpr(
	"CAST(LOCATION_ID AS INT) as DC_LOCATION_ID",
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_VIOLATION_DETAIL,'DC_NBR',dcnbr,f'{raw}.SLOT_VIOLATION_DETAIL')
#Shortcut_to_SLOT_VIOLATION_DETAIL.write.mode("overwrite").saveAsTable(f'{refine}.SLOT_VIOLATION_DETAIL')
Shortcut_to_SLOT_VIOLATION_DETAIL.createOrReplaceTempView('Shortcut_to_SLOT_VIOLATION_DETAIL')

# COMMAND ----------

spark.sql(f"""
MERGE INTO {legacy}.SLOT_VIOLATION_DETAIL target
USING Shortcut_to_SLOT_VIOLATION_DETAIL source
ON source.DC_LOCATION_ID = target.DC_LOCATION_ID and source.WEEK_DT = target.WEEK_DT 
WHEN MATCHED THEN
  DELETE """
)

