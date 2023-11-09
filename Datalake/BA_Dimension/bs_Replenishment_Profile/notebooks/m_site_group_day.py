# Databricks notebook source
#Code converted on 2023-09-26 15:05:29
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

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

# Processing node ASQ_Shortcut_to_SITE_GROUP_PRE, type SOURCE 
# COLUMN COUNT: 4

ASQ_Shortcut_to_SITE_GROUP_PRE = spark.sql(f"""SELECT
STORE_NBR,
SITE_GROUP_CD,
DELETE_IND,
SITE_GROUP_DESC
FROM {raw}.SITE_GROUP_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_to_SITE_GROUP_PRE = ASQ_Shortcut_to_SITE_GROUP_PRE \
	.withColumnRenamed(ASQ_Shortcut_to_SITE_GROUP_PRE.columns[0],'STORE_NBR') \
	.withColumnRenamed(ASQ_Shortcut_to_SITE_GROUP_PRE.columns[1],'SITE_GROUP_CD') \
	.withColumnRenamed(ASQ_Shortcut_to_SITE_GROUP_PRE.columns[2],'DELETE_IND') \
	.withColumnRenamed(ASQ_Shortcut_to_SITE_GROUP_PRE.columns[3],'SITE_GROUP_DESC')

# COMMAND ----------

# Processing node AGG_SITE_GROUP, type AGGREGATOR 
# COLUMN COUNT: 4

AGG_SITE_GROUP = ASQ_Shortcut_to_SITE_GROUP_PRE \
	.groupBy("STORE_NBR","SITE_GROUP_CD") \
	.agg( \
	first("DELETE_IND").alias("first_DELETE_IND"),
	first("SITE_GROUP_DESC").alias("first_SITE_GROUP_DESC")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_LOAD_DT, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
AGG_SITE_GROUP_temp = AGG_SITE_GROUP.toDF(*["AGG_SITE_GROUP___" + col for col in AGG_SITE_GROUP.columns])

EXP_LOAD_DT = AGG_SITE_GROUP_temp.selectExpr(
	"AGG_SITE_GROUP___sys_row_id as sys_row_id",
	"AGG_SITE_GROUP___STORE_NBR as STORE_NBR",
	"AGG_SITE_GROUP___SITE_GROUP_CD as SITE_GROUP_CD",
	"AGG_SITE_GROUP___first_DELETE_IND as first_DELETE_IND",
	"AGG_SITE_GROUP___first_SITE_GROUP_DESC as first_SITE_GROUP_DESC",
	"date_trunc ('dd', CURRENT_TIMESTAMP ) as LOAD_DT"
)

# COMMAND ----------

# Processing node Shortcut_To_SITE_GROUP_DAY, type TARGET 
# COLUMN COUNT: 5


Shortcut_To_SITE_GROUP_DAY = EXP_LOAD_DT.selectExpr(
	"CAST(STORE_NBR AS int) as STORE_NBR",
	"CAST(SITE_GROUP_CD AS STRING) as SITE_GROUP_CD",
	"CAST(first_DELETE_IND AS STRING) as DELETE_IND",
	"CAST(first_SITE_GROUP_DESC AS STRING) as SITE_GROUP_DESC",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
# Shortcut_To_SITE_GROUP_DAY.show()
#Shortcut_To_SITE_GROUP_DAY.write.saveAsTable(f'{legacy}.SITE_GROUP_DAY', mode = 'append')

# COMMAND ----------

try:
  primary_key = """source.STORE_NBR = target.STORE_NBR AND source.SITE_GROUP_CD = target.SITE_GROUP_CD """
  refined_perf_table = f"{legacy}.SITE_GROUP_DAY"
  Shortcut_To_SITE_GROUP_DAY.createOrReplaceTempView('Site_Group_Day_Source')
  #executeMerge(Shortcut_To_SITE_GROUP_DAY, refined_perf_table, primary_key)
  ##Generating Merge Query
  mergeQuery = f""" MERGE INTO {refined_perf_table} target USING Site_Group_Day_Source source ON {primary_key} WHEN MATCHED THEN UPDATE SET """
  targetColList = spark.read.table(refined_perf_table).columns
  for col in targetColList:
    mergeQuery = mergeQuery + " target." + col + "=source." + col + ","
  mergeQuery = mergeQuery.rstrip(",") + """ WHEN NOT MATCHED THEN INSERT ("""
  for col in targetColList:
    mergeQuery = mergeQuery + col + ","
  mergeQuery = mergeQuery.rstrip(",") + ") VALUES("
  for col in targetColList:
    mergeQuery = mergeQuery + "source." + col + ","
  mergeQuery = mergeQuery.rstrip(",") + ")"

  print(mergeQuery)

  spark.sql(mergeQuery)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SITE_GROUP_DAY", "SITE_GROUP_DAY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SITE_GROUP_DAY", "SITE_GROUP_DAY","Failed",str(e), f"{raw}.log_run_details", )
  raise e
