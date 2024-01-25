# Databricks notebook source
#Code converted on 2023-11-06 15:44:18
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

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_RANKING_WK, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SALES_RANKING_WK = spark.sql(f"""SELECT
WEEK_DT,
LOCATION_ID
FROM {legacy}.SALES_RANKING_WK""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_RANKING_WK = SQ_Shortcut_to_SALES_RANKING_WK \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_WK.columns[0],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_WK.columns[1],'LOCATION_ID')

# COMMAND ----------

# SQ_Shortcut_to_SALES_RANKING_WK.show()

# COMMAND ----------

# Processing node FIL_RECENT_TXNS, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_RANKING_WK_temp = SQ_Shortcut_to_SALES_RANKING_WK.toDF(*["SQ_Shortcut_to_SALES_RANKING_WK___" + col for col in SQ_Shortcut_to_SALES_RANKING_WK.columns])

FIL_RECENT_TXNS = SQ_Shortcut_to_SALES_RANKING_WK_temp.selectExpr(
	"SQ_Shortcut_to_SALES_RANKING_WK___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_SALES_RANKING_WK___LOCATION_ID as LOCATION_ID").filter("WEEK_DT > DATE_ADD(CURRENT_TIMESTAMP,-45)").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# FIL_RECENT_TXNS.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_RANKING_DATE_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SALES_RANKING_DATE_PRE = spark.sql(f"""SELECT
RANKING_WEEK_DT
FROM {raw}.SALES_RANKING_DATE_PRE""").distinct().withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
# SQ_Shortcut_to_SALES_RANKING_DATE_PRE = SQ_Shortcut_to_SALES_RANKING_DATE_PRE \
# 	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_DATE_PRE.columns[0],'') \
# 	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_DATE_PRE.columns[1],'RANKING_WEEK_DT')

# COMMAND ----------

# SQ_Shortcut_to_SALES_RANKING_DATE_PRE.show()

# COMMAND ----------

# Processing node AGG_PASS_PROCESSING_WK, type AGGREGATOR 
# COLUMN COUNT: 1
AGG_PASS_PROCESSING_WK = SQ_Shortcut_to_SALES_RANKING_DATE_PRE
# AGG_PASS_PROCESSING_WK = SQ_Shortcut_to_SALES_RANKING_DATE_PRE \
# 	.groupBy("RANKING_WEEK_DT") \
	# .withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SALES_RANKING_WK_DEL, type JOINER 
# COLUMN COUNT: 3

JNR_SALES_RANKING_WK_DEL = FIL_RECENT_TXNS.join(AGG_PASS_PROCESSING_WK,[FIL_RECENT_TXNS.WEEK_DT == AGG_PASS_PROCESSING_WK.RANKING_WEEK_DT],'inner')

# COMMAND ----------

# Processing node UPD_SALES_RANING_WK_DEL, type UPDATE_STRATEGY 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_SALES_RANKING_WK_DEL_temp = JNR_SALES_RANKING_WK_DEL.toDF(*["JNR_SALES_RANKING_WK_DEL___" + col for col in JNR_SALES_RANKING_WK_DEL.columns])

UPD_SALES_RANING_WK_DEL = JNR_SALES_RANKING_WK_DEL_temp.selectExpr(
	"JNR_SALES_RANKING_WK_DEL___WEEK_DT as WEEK_DT",
	"JNR_SALES_RANKING_WK_DEL___LOCATION_ID as LOCATION_ID") \
	.withColumn('pyspark_data_action', lit(2))

# COMMAND ----------

# UPD_SALES_RANING_WK_DEL.show()

# COMMAND ----------


# Processing node Shortcut_to_SALES_RANKING_WK1, type TARGET 
# COLUMN COUNT: 60


Shortcut_to_SALES_RANKING_WK1 = UPD_SALES_RANING_WK_DEL.selectExpr(
	"CAST(WEEK_DT AS DATE) as WEEK_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID"
)

Shortcut_to_SALES_RANKING_WK1.createOrReplaceTempView('SALES_RANKING_WK_DEL')

spark.sql(f"""
          MERGE INTO {legacy}.SALES_RANKING_WK trg
          USING SALES_RANKING_WK_DEL src
          ON (src.WEEK_DT =  trg.WEEK_DT AND src.LOCATION_ID =  trg.LOCATION_ID  )
          WHEN MATCHED THEN DELETE
          """)	

# COMMAND ----------


