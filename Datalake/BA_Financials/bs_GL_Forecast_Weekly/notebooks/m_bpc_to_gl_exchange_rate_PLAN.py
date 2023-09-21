# Databricks notebook source
#Code converted on 2023-08-17 15:36:59
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node LKP_GL_EXCHANGE_RATE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 4

LKP_GL_EXCHANGE_RATE_SRC = spark.sql(f"""SELECT
COMPANY,
PERIOD
FROM {legacy}.GL_EXCHANGE_RATE""")

# Conforming fields names to the component layout
LKP_GL_EXCHANGE_RATE_SRC = LKP_GL_EXCHANGE_RATE_SRC \
	.withColumnRenamed(LKP_GL_EXCHANGE_RATE_SRC.columns[0],'COMPANY') \
	.withColumnRenamed(LKP_GL_EXCHANGE_RATE_SRC.columns[1],'PERIOD') 

# COMMAND ----------

# Processing node SQ_Shortcut_to_GL_PLAN_PRE, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_GL_PLAN_PRE = spark.sql(f"""SELECT
FISCAL_MO,
GL_ACCT_NBR,
GL_CAT_CD,
STORE_NBR,
COMPANY_NBR,
CURRENCY_ID,
GL_AMT
FROM {raw}.GL_PLAN_PRE
WHERE CURRENCY_ID =  'USD'
ORDER BY 1,2,3,4,5""")  #.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_GL_PLAN_PRE1, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_GL_PLAN_PRE1 = spark.sql(f"""SELECT
FISCAL_MO,
GL_ACCT_NBR,
GL_CAT_CD,
STORE_NBR,
COMPANY_NBR,
CURRENCY_ID,
GL_AMT
FROM {raw}.GL_PLAN_PRE
WHERE CURRENCY_ID =  'CAD'
ORDER BY 1,2,3,4,5""")  #.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GL_PLAN_PRE1_temp = SQ_Shortcut_to_GL_PLAN_PRE1.toDF(*["SQ_Shortcut_to_GL_PLAN_PRE1___" + col for col in SQ_Shortcut_to_GL_PLAN_PRE1.columns])
SQ_Shortcut_to_GL_PLAN_PRE_temp = SQ_Shortcut_to_GL_PLAN_PRE.toDF(*["SQ_Shortcut_to_GL_PLAN_PRE___" + col for col in SQ_Shortcut_to_GL_PLAN_PRE.columns])

JNRTRANS = SQ_Shortcut_to_GL_PLAN_PRE1_temp.join(SQ_Shortcut_to_GL_PLAN_PRE_temp,[SQ_Shortcut_to_GL_PLAN_PRE1_temp.SQ_Shortcut_to_GL_PLAN_PRE1___FISCAL_MO == SQ_Shortcut_to_GL_PLAN_PRE_temp.SQ_Shortcut_to_GL_PLAN_PRE___FISCAL_MO, SQ_Shortcut_to_GL_PLAN_PRE1_temp.SQ_Shortcut_to_GL_PLAN_PRE1___GL_ACCT_NBR == SQ_Shortcut_to_GL_PLAN_PRE_temp.SQ_Shortcut_to_GL_PLAN_PRE___GL_ACCT_NBR, SQ_Shortcut_to_GL_PLAN_PRE1_temp.SQ_Shortcut_to_GL_PLAN_PRE1___GL_CAT_CD == SQ_Shortcut_to_GL_PLAN_PRE_temp.SQ_Shortcut_to_GL_PLAN_PRE___GL_CAT_CD, SQ_Shortcut_to_GL_PLAN_PRE1_temp.SQ_Shortcut_to_GL_PLAN_PRE1___STORE_NBR == SQ_Shortcut_to_GL_PLAN_PRE_temp.SQ_Shortcut_to_GL_PLAN_PRE___STORE_NBR, SQ_Shortcut_to_GL_PLAN_PRE1_temp.SQ_Shortcut_to_GL_PLAN_PRE1___COMPANY_NBR == SQ_Shortcut_to_GL_PLAN_PRE_temp.SQ_Shortcut_to_GL_PLAN_PRE___COMPANY_NBR],'right_outer').selectExpr(
	"SQ_Shortcut_to_GL_PLAN_PRE___FISCAL_MO as US_FISCAL_MO",
	"SQ_Shortcut_to_GL_PLAN_PRE___GL_ACCT_NBR as US_GL_ACCT_NBR",
	"SQ_Shortcut_to_GL_PLAN_PRE___GL_CAT_CD as US_GL_CAT_CD",
	"SQ_Shortcut_to_GL_PLAN_PRE___STORE_NBR as US_STORE_NBR",
	"SQ_Shortcut_to_GL_PLAN_PRE___COMPANY_NBR as US_COMPANY_NBR",
	"SQ_Shortcut_to_GL_PLAN_PRE___CURRENCY_ID as US_CURRENCY_ID",
	"SQ_Shortcut_to_GL_PLAN_PRE___GL_AMT as US_GL_AMT",
	"SQ_Shortcut_to_GL_PLAN_PRE1___FISCAL_MO as LOC_FISCAL_MO",
	"SQ_Shortcut_to_GL_PLAN_PRE1___GL_ACCT_NBR as LOC_GL_ACCT_NBR",
	"SQ_Shortcut_to_GL_PLAN_PRE1___GL_CAT_CD as LOC_GL_CAT_CD",
	"SQ_Shortcut_to_GL_PLAN_PRE1___STORE_NBR as LOC_STORE_NBR",
	"SQ_Shortcut_to_GL_PLAN_PRE1___COMPANY_NBR as LOC_COMPANY_NBR",
	"SQ_Shortcut_to_GL_PLAN_PRE1___CURRENCY_ID as LOC_CURRENCY_ID",
	"SQ_Shortcut_to_GL_PLAN_PRE1___GL_AMT as LOC_GL_AMT").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# # Processing node EXPTRANS, type EXPRESSION 
# # COLUMN COUNT: 3


# EXPTRANS = JNRTRANS.selectExpr(
# 	"sys_row_id as sys_row_id",
# 	"US_FISCAL_MO as US_FISCAL_MO",
# 	"US_COMPANY_NBR as US_COMPANY_NBR",
# 	"CASE WHEN LOC_GL_AMT > 0.0 THEN ROUND(LOC_GL_AMT / US_GL_AMT, 6) ELSE 1.00000 END as RATE"
# )


# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 3



EXPTRANS = JNRTRANS.selectExpr(
	"sys_row_id as sys_row_id",
  "US_GL_ACCT_NBR as US_GL_ACCT_NBR" ,
  "US_GL_CAT_CD as US_GL_CAT_CD",
   "US_STORE_NBR as US_STORE_NBR",
	"US_FISCAL_MO as US_FISCAL_MO",
	"US_COMPANY_NBR as US_COMPANY_NBR",
	"CASE WHEN LOC_GL_AMT > 0.0 THEN ROUND(LOC_GL_AMT / US_GL_AMT, 6) ELSE 1.00000 END as RATE"
)
EXPTRANS.createOrReplaceTempView('EXPTRANS')

# COMMAND ----------

# # Processing node AGGTRANS, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 3

# AGGTRANS = EXPTRANS.selectExpr(
# 	"US_COMPANY_NBR as COMPANY",
# 	"US_FISCAL_MO as PERIOD",
# 	"RATE as in_RATE") \
# 	.groupBy("COMPANY","PERIOD") \
# 	.agg(first("in_RATE").alias("RATE")) \
# 	.withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------


# Processing node AGGTRANS, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3
windowPartition = Window.partitionBy("COMPANY","PERIOD").orderBy("PERIOD","US_GL_ACCT_NBR","US_GL_CAT_CD","US_STORE_NBR","COMPANY")

AGGTRANS = EXPTRANS.selectExpr(
	"US_COMPANY_NBR as COMPANY",
	"US_FISCAL_MO as PERIOD",
 "US_GL_ACCT_NBR as US_GL_ACCT_NBR",
 "US_GL_CAT_CD as US_GL_CAT_CD",
 "US_STORE_NBR as US_STORE_NBR",
	"RATE as in_RATE") \
	.withColumn("RATE",first("in_RATE").over(windowPartition)) \
	.withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node LKP_GL_EXCHANGE_RATE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


LKP_GL_EXCHANGE_RATE_lookup_result = AGGTRANS.selectExpr(
  "sys_row_id",
  "RATE AS FORECAST_EXCH_RATE",
  "COMPANY as in_COMPANY",
  "PERIOD as in_PERIOD").join(LKP_GL_EXCHANGE_RATE_SRC, (col('COMPANY') == col('in_COMPANY')) & (col('PERIOD') == col('in_PERIOD')), 'left') \
.withColumn('row_num_COMPANY', row_number().over(Window.partitionBy("sys_row_id").orderBy("COMPANY")))

 
UPD_upd_insert = LKP_GL_EXCHANGE_RATE_lookup_result.selectExpr(
  "in_COMPANY as COMPANY",
  "in_PERIOD as PERIOD",
  "FORECAST_EXCH_RATE as FORECAST_EXCH_RATE",
  "CASE WHEN COMPANY IS NULL THEN 0 ELSE 1 END as pyspark_data_action")
  

# COMMAND ----------

# Processing node Shortcut_to_GL_EXCHANGE_RATE1, type TARGET
# COLUMN COUNT: 4
 
 
Shortcut_to_GL_EXCHANGE_RATE1 = UPD_upd_insert.selectExpr(
  "CAST(COMPANY AS INT) as COMPANY",
  "CAST(PERIOD AS INT) as PERIOD",
  "CAST(NULL AS DECIMAL(9,6)) as FORECAST_EXCH_RATE",
  "CAST(FORECAST_EXCH_RATE AS DECIMAL(9,6)) as F1_ADJ_EXCH_RATE",
  "pyspark_data_action as pyspark_data_action"
)

Shortcut_to_GL_EXCHANGE_RATE1.dropDuplicates().createOrReplaceTempView('Shortcut_to_GL_EXCHANGE_RATE1')
Shortcut_to_GL_EXCHANGE_RATE1.dropDuplicates().drop('pyspark_data_action').createOrReplaceTempView('Shortcut_to_GL_EXCHANGE_RATE1_ins')


# try:
#   primary_key = """source.COMPANY = target.COMPANY AND source.PERIOD = target.PERIOD"""
#   refined_perf_table = f"{legacy}.GL_EXCHANGE_RATE"
#   executeMerge(Shortcut_to_GL_EXCHANGE_RATE1, refined_perf_table, primary_key)
#   logger.info(f"Merge with {refined_perf_table} completed]")
#   logPrevRunDt("GL_EXCHANGE_RATE", "GL_EXCHANGE_RATE", "Completed", "N/A", f"{raw}.log_run_details")
# except Exception as e:
#   logPrevRunDt("GL_EXCHANGE_RATE", "GL_EXCHANGE_RATE","Failed",str(e), f"{raw}.log_run_details", )
#   raise e


# COMMAND ----------

try:
 
  spark.sql(f"""
      MERGE INTO {legacy}.GL_EXCHANGE_RATE target
      USING Shortcut_to_GL_EXCHANGE_RATE1_ins source
      ON source.COMPANY = target.COMPANY AND source.PERIOD = target.PERIOD
      when not matched then
      insert *
      """)
  logger.info(f"Merge with GL_EXCHANGE_RATE completed]")
  logPrevRunDt("GL_EXCHANGE_RATE", "GL_EXCHANGE_RATE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GL_EXCHANGE_RATE", "GL_EXCHANGE_RATE","Failed",str(e), f"{raw}.log_run_details", )
  raise e

# COMMAND ----------

try:
 
  spark.sql(f"""
      MERGE INTO {legacy}.GL_EXCHANGE_RATE target
      USING Shortcut_to_GL_EXCHANGE_RATE1 source
      ON source.COMPANY = target.COMPANY AND source.PERIOD = target.PERIOD
      when matched then
      update set 
        COMPANY=source.COMPANY,
        PERIOD=source.PERIOD,
        F1_ADJ_EXCH_RATE=source.F1_ADJ_EXCH_RATE
      """)
  logger.info(f"Merge with GL_EXCHANGE_RATE completed]")
  logPrevRunDt("GL_EXCHANGE_RATE", "GL_EXCHANGE_RATE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("GL_EXCHANGE_RATE", "GL_EXCHANGE_RATE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
