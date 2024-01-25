# Databricks notebook source
#Code converted on 2023-11-06 15:44:13
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

# Processing node SQ_Shortcut_to_SALES_RANKING_SALES_PRE, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_SALES_RANKING_SALES_PRE = spark.sql(f"""SELECT
WEEK_DT,
TOTAL_52WK_SALES_AMT,
MERCH_52WK_SALES_AMT,
SERVICES_52WK_SALES_AMT,
SALON_52WK_SALES_AMT,
TRAINING_52WK_SALES_AMT,
HOTEL_DDC_52WK_SALES_AMT,
CONSUMABLES_52WK_SALES_AMT,
HARDGOODS_52WK_SALES_AMT,
SPECIALTY_52WK_SALES_AMT,
COMP_CURR_FLAG,
SALES_CURR_FLAG,
LOCATION_TYPE_ID
FROM {raw}.SALES_RANKING_SALES_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_RANKING_SALES_PRE = SQ_Shortcut_to_SALES_RANKING_SALES_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[0],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[1],'TOTAL_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[2],'MERCH_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[3],'SERVICES_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[4],'SALON_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[5],'TRAINING_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[6],'HOTEL_DDC_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[7],'CONSUMABLES_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[8],'HARDGOODS_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[9],'SPECIALTY_52WK_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[10],'COMP_CURR_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[11],'SALES_CURR_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns[12],'LOCATION_TYPE_ID')

# COMMAND ----------

# SQ_Shortcut_to_SALES_RANKING_SALES_PRE.show()

# COMMAND ----------

# Processing node FIL_ONLY_NON_DOTCOM_STORES, type FILTER 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_RANKING_SALES_PRE_temp = SQ_Shortcut_to_SALES_RANKING_SALES_PRE.toDF(*["SQ_Shortcut_to_SALES_RANKING_SALES_PRE___" + col for col in SQ_Shortcut_to_SALES_RANKING_SALES_PRE.columns])

FIL_ONLY_NON_DOTCOM_STORES = SQ_Shortcut_to_SALES_RANKING_SALES_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___TOTAL_52WK_SALES_AMT as TOTAL_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___MERCH_52WK_SALES_AMT as MERCH_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___SERVICES_52WK_SALES_AMT as SERVICES_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___SALON_52WK_SALES_AMT as SALON_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___TRAINING_52WK_SALES_AMT as TRAINING_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___HOTEL_DDC_52WK_SALES_AMT as HOTEL_DDC_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___CONSUMABLES_52WK_SALES_AMT as CONSUMABLES_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___HARDGOODS_52WK_SALES_AMT as HARDGOODS_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___SPECIALTY_52WK_SALES_AMT as SPECIALTY_52WK_SALES_AMT",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___COMP_CURR_FLAG as COMP_CURR_FLAG",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___SALES_CURR_FLAG as SALES_CURR_FLAG",
	"SQ_Shortcut_to_SALES_RANKING_SALES_PRE___LOCATION_TYPE_ID as LOCATION_TYPE_ID").filter("COMP_CURR_FLAG = 1  AND LOCATION_TYPE_ID = 8 ").withColumn("sys_row_id", monotonically_increasing_id())


# .filter("COMP_CURR_FLAG = 1 - - Only comparable stores AND LOCATION_TYPE_ID = 8 - - only stores no dot.com")

# COMMAND ----------

 # Processing node AGG_WEEK_TOTALS, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

AGG_WEEK_TOTALS = FIL_ONLY_NON_DOTCOM_STORES.selectExpr(
	"WEEK_DT as WEEK_DT",
	"TOTAL_52WK_SALES_AMT as in_TOTAL_52WK_SALES_AMT",
	"MERCH_52WK_SALES_AMT as in_MERCH_52WK_SALES_AMT",
	"SERVICES_52WK_SALES_AMT as in_SERVICES_52WK_SALES_AMT",
	"SALON_52WK_SALES_AMT as in_SALON_52WK_SALES_AMT",
	"TRAINING_52WK_SALES_AMT as in_TRAINING_52WK_SALES_AMT",
	"HOTEL_DDC_52WK_SALES_AMT as in_HOTEL_DDC_52WK_SALES_AMT",
	"CONSUMABLES_52WK_SALES_AMT as in_CONSUMABLES_52WK_SALES_AMT",
	"HARDGOODS_52WK_SALES_AMT as in_HARDGOODS_52WK_SALES_AMT",
	"SPECIALTY_52WK_SALES_AMT as in_SPECIALTY_52WK_SALES_AMT") \
	.groupBy("WEEK_DT") \
	.agg( \
		when((sum(col('in_TOTAL_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_TOTAL_52WK_SALES_AMT'))).alias("TOTAL_52WK_SALES_AMT"),
		when((sum(col('in_MERCH_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_MERCH_52WK_SALES_AMT'))).alias("MERCH_52WK_SALES_AMT"),
		when((sum(col('in_SERVICES_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_SERVICES_52WK_SALES_AMT'))).alias("SERVICES_52WK_SALES_AMT"),
		when((sum(col('in_SALON_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_SALON_52WK_SALES_AMT'))).alias("SALON_52WK_SALES_AMT"),
		when((sum(col('in_TRAINING_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_TRAINING_52WK_SALES_AMT'))).alias("TRAINING_52WK_SALES_AMT"),
		when((sum(col('in_HOTEL_DDC_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_HOTEL_DDC_52WK_SALES_AMT'))).alias("HOTEL_DDC_52WK_SALES_AMT"),
		when((sum(col('in_CONSUMABLES_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_CONSUMABLES_52WK_SALES_AMT'))).alias("CONSUMABLES_52WK_SALES_AMT"),
		when((sum(col('in_HARDGOODS_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_HARDGOODS_52WK_SALES_AMT'))).alias("HARDGOODS_52WK_SALES_AMT"),
		when((sum(col('in_SPECIALTY_52WK_SALES_AMT')) .isNull()) ,(lit(0))) .otherwise(sum(col('in_SPECIALTY_52WK_SALES_AMT'))).alias("SPECIALTY_52WK_SALES_AMT")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_SALES_RANKING_TOTALS_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_SALES_RANKING_TOTALS_PRE = AGG_WEEK_TOTALS.selectExpr(
	"CAST(WEEK_DT AS DATE) as WEEK_DT",
	"CAST(TOTAL_52WK_SALES_AMT AS DECIMAL(18,2)) as TOTAL_52WK_COMP_STORES_AMT",
	"CAST(MERCH_52WK_SALES_AMT AS DECIMAL(18,2)) as MERCH_52WK_COMP_STORES_AMT",
	"CAST(SERVICES_52WK_SALES_AMT AS DECIMAL(18,2)) as SERVICES_52WK_COMP_STORES_AMT",
	"CAST(SALON_52WK_SALES_AMT AS DECIMAL(18,2)) as SALON_52WK_COMP_STORES_AMT",
	"CAST(TRAINING_52WK_SALES_AMT AS DECIMAL(18,2)) as TRAINING_52WK_COMP_STORES_AMT",
	"CAST(HOTEL_DDC_52WK_SALES_AMT AS DECIMAL(18,2)) as HOTEL_DDC_52WK_COMP_STORES_AMT",
	"CAST(CONSUMABLES_52WK_SALES_AMT AS DECIMAL(18,2)) as CONSUMABLES_52WK_COMP_STORES_AMT",
	"CAST(HARDGOODS_52WK_SALES_AMT AS DECIMAL(18,2)) as HARDGOODS_52WK_COMP_STORES_AMT",
	"CAST(SPECIALTY_52WK_SALES_AMT AS DECIMAL(18,2)) as SPECIALTY_52WK_COMP_STORES_AMT"
)
try:
    # chk=DuplicateChecker()
    # chk.check_for_duplicate_primary_keys(Shortcut_to_SALES_RANKING_TOTALS_PRE,[key])
    Shortcut_to_SALES_RANKING_TOTALS_PRE.write.saveAsTable(f'{raw}.SALES_RANKING_TOTALS_PRE', mode = 'overwrite')
except Exception as e:
    raise e

# COMMAND ----------


