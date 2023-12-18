# Databricks notebook source
#Code converted on 2023-12-08 12:31:59
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
target_bucket=getParameterValue(raw,'wf_NZ2ORA_SC_RP_Replication','m_NZ2ORA_Replenishment_profile_flat_file','target_bucket')
target_file=getParameterValue(raw,'wf_NZ2ORA_SC_RP_Replication','m_NZ2ORA_Replenishment_profile_flat_file','target_file')
print(target_bucket, target_file)


# COMMAND ----------

current_date = datetime.today().strftime('%Y%m%d')
target_bucket = target_bucket.strip("/") + "/" + current_date
print(target_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_REPLENISHMENT_PROFILE, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_REPLENISHMENT_PROFILE = spark.sql(f"""SELECT
PRODUCT_ID,
LOCATION_ID,
ROUND_VALUE_QTY,
SAFETY_QTY,
SERVICE_LVL_RT,
REORDER_POINT_QTY,
PLAN_DELIV_DAYS,
ROUND_PROFILE_CD,
TARGET_STOCK_QTY,
PRESENT_QTY,
POG_CAPACITY_QTY,
POG_FACINGS_QTY,
PROMO_QTY,
BASIC_VALUE_QTY,
LAST_FC_DT,
LOAD_DT
FROM {legacy}.REPLENISHMENT_PROFILE""")
# .withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_REPLENISHMENT_PROFILE = SQ_Shortcut_to_REPLENISHMENT_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[2],'ROUND_VALUE_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[3],'SAFETY_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[4],'SERVICE_LVL_RT') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[5],'REORDER_POINT_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[6],'PLAN_DELIV_DAYS') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[7],'ROUND_PROFILE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[8],'TARGET_STOCK_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[9],'PRESENT_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[10],'POG_CAPACITY_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[11],'POG_FACINGS_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[12],'PROMO_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[13],'BASIC_VALUE_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[14],'LAST_FC_DT') \
	.withColumnRenamed(SQ_Shortcut_to_REPLENISHMENT_PROFILE.columns[15],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_REPLENISHMENT_PROFILE_flat_file, type TARGET 
# COLUMN COUNT: 16

Shortcut_to_REPLENISHMENT_PROFILE_flat_file = SQ_Shortcut_to_REPLENISHMENT_PROFILE.selectExpr(
	"CASE WHEN PRODUCT_ID IS NULL THEN RPAD('*', 10) ELSE LPAD(PRODUCT_ID::string, 10) END as PRODUCT_ID",
	"CASE WHEN LOCATION_ID IS NULL THEN RPAD('*', 10) ELSE LPAD(LOCATION_ID::string, 10) END as LOCATION_ID",
	"CASE WHEN ROUND_VALUE_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(ROUND_VALUE_QTY::string,10 ) END as ROUND_VALUE_QTY",
	"CASE WHEN SAFETY_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(SAFETY_QTY::string,10 ) END as SAFETY_QTY",
	"CASE WHEN SERVICE_LVL_RT IS NULL THEN RPAD('*', 5) ELSE LPAD(SERVICE_LVL_RT::string, 5) END as SERVICE_LVL_RT",
	"CASE WHEN REORDER_POINT_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(REORDER_POINT_QTY::string, 10) END as REORDER_POINT_QTY",
	"CASE WHEN PLAN_DELIV_DAYS IS NULL THEN RPAD('*', 5) ELSE LPAD(PLAN_DELIV_DAYS::string, 5) END as PLAN_DELIV_DAYS",
	"CASE WHEN ROUND_PROFILE_CD IS NULL THEN RPAD('*', 4) ELSE LPAD(ROUND_PROFILE_CD::string, 4) END as ROUND_PROFILE_CD",
	"CASE WHEN TARGET_STOCK_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(TARGET_STOCK_QTY::string, 10) END as TARGET_STOCK_QTY",
	"CASE WHEN PRESENT_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(PRESENT_QTY::string, 10) END as PRESENT_QTY",
	"CASE WHEN POG_CAPACITY_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(POG_CAPACITY_QTY::string,10 ) END as POG_CAPACITY_QTY",
	"CASE WHEN POG_FACINGS_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(POG_FACINGS_QTY::string,10 ) END as POG_FACINGS_QTY",
	"CASE WHEN PROMO_QTY IS NULL THEN RPAD('*', 10) ELSE LPAD(PROMO_QTY::string,10 ) END as PROMO_QTY",	
    "CASE WHEN BASIC_VALUE_QTY IS NULL THEN RPAD('*', 13) ELSE LPAD(BASIC_VALUE_QTY::string, 13) END as BASIC_VALUE_QTY",	
    "CASE WHEN LAST_FC_DT IS NULL THEN RPAD('*', 19) ELSE LPAD(date_format(LAST_FC_DT, 'MM/dd/yyyy HH:mm:ss')::string ,19 ) END as LAST_FC_DT",
    "CASE WHEN LOAD_DT IS NULL THEN RPAD('*', 19) ELSE LPAD(date_format(LOAD_DT, 'MM/dd/yyyy HH:mm:ss')::string ,19 ) END as LOAD_DT",
)
cols = Shortcut_to_REPLENISHMENT_PROFILE_flat_file.schema.names

Shortcut_to_REPLENISHMENT_PROFILE_flat_file_text = Shortcut_to_REPLENISHMENT_PROFILE_flat_file.select(concat(*cols).alias('value'))


# COMMAND ----------

Shortcut_to_REPLENISHMENT_PROFILE_flat_file_text.repartition(1).write.mode('overwrite').text(target_bucket.strip("/") + "/" + target_file[:-4])

removeTransactionFiles(target_bucket.strip("/") + "/" + target_file[:-4])
newFilePath = target_bucket.strip("/") + "/" + target_file[:-4]
print(newFilePath)
print(target_bucket)
renamePartFileNames(newFilePath, newFilePath,'.dat')
