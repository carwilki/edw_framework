# Databricks notebook source
#Code converted on 2023-11-06 15:44:10
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

# Processing node SQ_Shortcut_to_SLSCMP_STORE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SLSCMP_STORE = spark.sql(f"""SELECT
LOCATION_ID,
SALES_CURR_FLAG,
COMP_CURR_FLAG
FROM {legacy}.SLSCMP_STORE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SLSCMP_STORE = SQ_Shortcut_to_SLSCMP_STORE \
	.withColumnRenamed(SQ_Shortcut_to_SLSCMP_STORE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SLSCMP_STORE.columns[1],'SALES_CURR_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_SLSCMP_STORE.columns[2],'COMP_CURR_FLAG')

# COMMAND ----------

# SQ_Shortcut_to_SLSCMP_STORE.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE = spark.sql(f"""SELECT
PRODUCT_ID,
SAP_DEPT_ID
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SKU_PROFILE = SQ_Shortcut_to_SKU_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE.columns[1],'SAP_DEPT_ID')

# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT = spark.sql(f"""SELECT
PRODUCT_ID,
LOCATION_ID,
WEEK_DT,
NET_SALES_AMT,
EXCH_RATE_PCT
 FROM {legacy}.SALES_DAY_SKU_STORE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())
#FROM nzmigration.wf_sales_ranking_wk_12_15_sales_day_sku_store_rpt_v1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT = SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[2],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[3],'NET_SALES_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns[4],'EXCH_RATE_PCT')

# COMMAND ----------

# SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.show()


# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_RANKING_DATE_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SALES_RANKING_DATE_PRE = spark.sql(f"""SELECT
WEEK_DT,
RANKING_WEEK_DT
FROM {raw}.SALES_RANKING_DATE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SALES_RANKING_DATE_PRE = SQ_Shortcut_to_SALES_RANKING_DATE_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_DATE_PRE.columns[0],'WEEK_DT') \
	.withColumnRenamed(SQ_Shortcut_to_SALES_RANKING_DATE_PRE.columns[1],'RANKING_WEEK_DT')

# COMMAND ----------

# SQ_Shortcut_to_SALES_RANKING_DATE_PRE.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_USR_STORE_DIVISIONS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_USR_STORE_DIVISIONS = spark.sql(f"""SELECT
SAP_DEPT_ID,
STORE_DIVISION_ID
FROM {legacy}.USR_STORE_DIVISIONS""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_USR_STORE_DIVISIONS = SQ_Shortcut_to_USR_STORE_DIVISIONS \
	.withColumnRenamed(SQ_Shortcut_to_USR_STORE_DIVISIONS.columns[0],'SAP_DEPT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_USR_STORE_DIVISIONS.columns[1],'STORE_DIVISION_ID')

# COMMAND ----------

# SQ_Shortcut_to_USR_STORE_DIVISIONS.show()

# COMMAND ----------

# Processing node JNR_WEEKS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp = SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.toDF(*["SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___" + col for col in SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns])
SQ_Shortcut_to_SALES_RANKING_DATE_PRE_temp = SQ_Shortcut_to_SALES_RANKING_DATE_PRE.toDF(*["SQ_Shortcut_to_SALES_RANKING_DATE_PRE___" + col for col in SQ_Shortcut_to_SALES_RANKING_DATE_PRE.columns])

JNR_WEEKS = SQ_Shortcut_to_SALES_RANKING_DATE_PRE_temp.join(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp,[SQ_Shortcut_to_SALES_RANKING_DATE_PRE_temp.SQ_Shortcut_to_SALES_RANKING_DATE_PRE___WEEK_DT == SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp.SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___WEEK_DT],'inner').selectExpr(
	"SQ_Shortcut_to_SALES_RANKING_DATE_PRE___WEEK_DT as WEEK_DT_wk",
	"SQ_Shortcut_to_SALES_RANKING_DATE_PRE___RANKING_WEEK_DT as RANKING_WEEK_DT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___NET_SALES_AMT as NET_SALES_AMT",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___EXCH_RATE_PCT as EXCH_RATE_PCT")

# COMMAND ----------

# JNR_WEEKS.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
LOCATION_ID,
LOCATION_TYPE_ID
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SITE_PROFILE.columns[1],'LOCATION_TYPE_ID')

# COMMAND ----------

# SQ_Shortcut_to_SITE_PROFILE.show()

# COMMAND ----------

# Processing node JNR_SLS_CMP, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_WEEKS_temp = JNR_WEEKS.toDF(*["JNR_WEEKS___" + col for col in JNR_WEEKS.columns])
SQ_Shortcut_to_SLSCMP_STORE_temp = SQ_Shortcut_to_SLSCMP_STORE.toDF(*["SQ_Shortcut_to_SLSCMP_STORE___" + col for col in SQ_Shortcut_to_SLSCMP_STORE.columns])

JNR_SLS_CMP = SQ_Shortcut_to_SLSCMP_STORE_temp.join(JNR_WEEKS_temp,[SQ_Shortcut_to_SLSCMP_STORE_temp.SQ_Shortcut_to_SLSCMP_STORE___LOCATION_ID == JNR_WEEKS_temp.JNR_WEEKS___LOCATION_ID],'inner').selectExpr(
	"JNR_WEEKS___RANKING_WEEK_DT as RANKING_WEEK_DT",
	"JNR_WEEKS___PRODUCT_ID as PRODUCT_ID",
	"JNR_WEEKS___LOCATION_ID as LOCATION_ID",
	"JNR_WEEKS___NET_SALES_AMT as NET_SALES_AMT",
	"JNR_WEEKS___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"SQ_Shortcut_to_SLSCMP_STORE___LOCATION_ID as LOCATION_ID_cmp",
	"SQ_Shortcut_to_SLSCMP_STORE___COMP_CURR_FLAG as COMP_CURR_FLAG",
	"SQ_Shortcut_to_SLSCMP_STORE___SALES_CURR_FLAG as SALES_CURR_FLAG")

# COMMAND ----------

# JNR_SLS_CMP.show()

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])
JNR_SLS_CMP_temp = JNR_SLS_CMP.toDF(*["JNR_SLS_CMP___" + col for col in JNR_SLS_CMP.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(JNR_SLS_CMP_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID == JNR_SLS_CMP_temp.JNR_SLS_CMP___LOCATION_ID],'inner').selectExpr(
	"JNR_SLS_CMP___RANKING_WEEK_DT as RANKING_WEEK_DT",
	"JNR_SLS_CMP___PRODUCT_ID as PRODUCT_ID",
	"JNR_SLS_CMP___LOCATION_ID as LOCATION_ID",
	"JNR_SLS_CMP___NET_SALES_AMT as NET_SALES_AMT",
	"JNR_SLS_CMP___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"JNR_SLS_CMP___COMP_CURR_FLAG as COMP_CURR_FLAG",
	"JNR_SLS_CMP___SALES_CURR_FLAG as SALES_CURR_FLAG",
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID_site",
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_TYPE_ID as LOCATION_TYPE_ID")

# COMMAND ----------

# JNR_SITE_PROFILE.show()

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_SKU_PROFILE_temp = SQ_Shortcut_to_SKU_PROFILE.toDF(*["SQ_Shortcut_to_SKU_PROFILE___" + col for col in SQ_Shortcut_to_SKU_PROFILE.columns])

JNR_SKU_PROFILE = SQ_Shortcut_to_SKU_PROFILE_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_SKU_PROFILE_temp.SQ_Shortcut_to_SKU_PROFILE___PRODUCT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___PRODUCT_ID],'inner').selectExpr(
	"JNR_SITE_PROFILE___RANKING_WEEK_DT as RANKING_WEEK_DT",
	"JNR_SITE_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SITE_PROFILE___NET_SALES_AMT as NET_SALES_AMT",
	"JNR_SITE_PROFILE___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"JNR_SITE_PROFILE___COMP_CURR_FLAG as COMP_CURR_FLAG",
	"JNR_SITE_PROFILE___SALES_CURR_FLAG as SALES_CURR_FLAG",
	"JNR_SITE_PROFILE___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"SQ_Shortcut_to_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID_sku",
	"SQ_Shortcut_to_SKU_PROFILE___SAP_DEPT_ID as SAP_DEPT_ID")

# COMMAND ----------

# JNR_SKU_PROFILE.show()

# COMMAND ----------

# Processing node JNR_USR_STORE_DIVISIONS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_STORE_DIVISIONS_temp = SQ_Shortcut_to_USR_STORE_DIVISIONS.toDF(*["SQ_Shortcut_to_USR_STORE_DIVISIONS___" + col for col in SQ_Shortcut_to_USR_STORE_DIVISIONS.columns])
JNR_SKU_PROFILE_temp = JNR_SKU_PROFILE.toDF(*["JNR_SKU_PROFILE___" + col for col in JNR_SKU_PROFILE.columns])

JNR_USR_STORE_DIVISIONS = SQ_Shortcut_to_USR_STORE_DIVISIONS_temp.join(JNR_SKU_PROFILE_temp,[SQ_Shortcut_to_USR_STORE_DIVISIONS_temp.SQ_Shortcut_to_USR_STORE_DIVISIONS___SAP_DEPT_ID == JNR_SKU_PROFILE_temp.JNR_SKU_PROFILE___SAP_DEPT_ID],'inner').selectExpr(
	"JNR_SKU_PROFILE___RANKING_WEEK_DT as RANKING_WEEK_DT",
	"JNR_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE___NET_SALES_AMT as NET_SALES_AMT",
	"JNR_SKU_PROFILE___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"JNR_SKU_PROFILE___COMP_CURR_FLAG as COMP_CURR_FLAG",
	"JNR_SKU_PROFILE___SALES_CURR_FLAG as SALES_CURR_FLAG",
	"JNR_SKU_PROFILE___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"JNR_SKU_PROFILE___SAP_DEPT_ID as SAP_DEPT_ID",
	"SQ_Shortcut_to_USR_STORE_DIVISIONS___SAP_DEPT_ID as SAP_DEPT_ID_store_div",
	"SQ_Shortcut_to_USR_STORE_DIVISIONS___STORE_DIVISION_ID as STORE_DIVISION_ID")

# COMMAND ----------

# JNR_USR_STORE_DIVISIONS.show()

# COMMAND ----------

# Processing node EXP_CONV_LOCAL_DOLLARS, type EXPRESSION 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_USR_STORE_DIVISIONS_temp = JNR_USR_STORE_DIVISIONS.toDF(*["JNR_USR_STORE_DIVISIONS___" + col for col in JNR_USR_STORE_DIVISIONS.columns])

EXP_CONV_LOCAL_DOLLARS = JNR_USR_STORE_DIVISIONS_temp.selectExpr(
	# "JNR_USR_STORE_DIVISIONS___sys_row_id as sys_row_id",
	"JNR_USR_STORE_DIVISIONS___RANKING_WEEK_DT as RANKING_WEEK_DT",
	"JNR_USR_STORE_DIVISIONS___LOCATION_ID as LOCATION_ID",
	"JNR_USR_STORE_DIVISIONS___NET_SALES_AMT as NET_SALES_AMT",
	"JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"JNR_USR_STORE_DIVISIONS___COMP_CURR_FLAG as COMP_CURR_FLAG",
	"JNR_USR_STORE_DIVISIONS___SALES_CURR_FLAG as SALES_CURR_FLAG",
	"JNR_USR_STORE_DIVISIONS___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
	"JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID as STORE_DIVISION_ID",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 1, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as TOTAL_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 2, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as MERCH_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 5, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as SERVICES_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 6, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as SALON_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 7, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as TRAINING_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 8, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as HOTEL_DDC_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 9, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as CONSUMABLES_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 10, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as HARDGOODS_SALES",
	"IF (JNR_USR_STORE_DIVISIONS___STORE_DIVISION_ID = 3, JNR_USR_STORE_DIVISIONS___NET_SALES_AMT * JNR_USR_STORE_DIVISIONS___EXCH_RATE_PCT, 0) as SPECIALTY_SALES"
)

# COMMAND ----------

# EXP_CONV_LOCAL_DOLLARS.show()

# COMMAND ----------

# Processing node AGG_52_TOTALS, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

AGG_52_TOTALS = EXP_CONV_LOCAL_DOLLARS.selectExpr(
	"RANKING_WEEK_DT as in_RANKING_WEEK_DT",
	"LOCATION_ID as LOCATION_ID",
	"TOTAL_SALES as in_TOTAL_SALES",
	"MERCH_SALES as in_MERCH_SALES",
	"SERVICES_SALES as in_SERVICES_SALES",
	"SALON_SALES as in_SALON_SALES",
	"TRAINING_SALES as in_TRAINING_SALES",
	"HOTEL_DDC_SALES as in_HOTEL_DDC_SALES",
	"CONSUMABLES_SALES as in_CONSUMABLES_SALES",
	"HARDGOODS_SALES as in_HARDGOODS_SALES",
	"SPECIALTY_SALES as in_SPECIALTY_SALES",
	"COMP_CURR_FLAG as in_COMP_CURR_FLAG",
	"SALES_CURR_FLAG as in_SALES_CURR_FLAG",
	"LOCATION_TYPE_ID as in_LOCATION_TYPE_ID") \
	.groupBy("LOCATION_ID") \
	.agg( \
	max(col('in_RANKING_WEEK_DT')).alias("RANKING_WEEK_DT1"),
	round(sum(col('in_TOTAL_SALES')) , 2).alias("TOTAL_SALES"),
	round(sum(col('in_MERCH_SALES')) , 2).alias("MERCH_SALES"),
	round(sum(col('in_SERVICES_SALES')) , 2).alias("SERVICES_SALES"),
	round(sum(col('in_SALON_SALES')) , 2).alias("SALON_SALES"),
	round(sum(col('in_TRAINING_SALES')) , 2).alias("TRAINING_SALES"),
	round(sum(col('in_HOTEL_DDC_SALES')) , 2).alias("HOTEL_DDC_SALES"),
	round(sum(col('in_CONSUMABLES_SALES')) , 2).alias("CONSUMABLES_SALES"),
	round(sum(col('in_HARDGOODS_SALES')) , 2).alias("HARDGOODS_SALES"),
	round(sum(col('in_SPECIALTY_SALES')) , 2).alias("SPECIALTY_SALES"),
	max(col('in_COMP_CURR_FLAG')).alias("COMP_CURR_FLAG"),
	max(col('in_SALES_CURR_FLAG')).alias("SALES_CURR_FLAG"),
	max(col('in_LOCATION_TYPE_ID')).alias("LOCATION_TYPE_ID")
	) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# AGG_52_TOTALS.show()

# COMMAND ----------

# Processing node Shortcut_to_SALES_RANKING_SALES_PRE, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_SALES_RANKING_SALES_PRE = AGG_52_TOTALS.selectExpr(
	"CAST(RANKING_WEEK_DT1 AS DATE) as WEEK_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(TOTAL_SALES AS DECIMAL(18,2)) as TOTAL_52WK_SALES_AMT",
	"CAST(MERCH_SALES AS DECIMAL(18,2)) as MERCH_52WK_SALES_AMT",
	"CAST(SERVICES_SALES AS DECIMAL(18,2)) as SERVICES_52WK_SALES_AMT",
	"CAST(SALON_SALES AS DECIMAL(18,2)) as SALON_52WK_SALES_AMT",
	"CAST(TRAINING_SALES AS DECIMAL(18,2)) as TRAINING_52WK_SALES_AMT",
	"CAST(HOTEL_DDC_SALES AS DECIMAL(18,2)) as HOTEL_DDC_52WK_SALES_AMT",
	"CAST(CONSUMABLES_SALES AS DECIMAL(18,2)) as CONSUMABLES_52WK_SALES_AMT",
	"CAST(HARDGOODS_SALES AS DECIMAL(18,2)) as HARDGOODS_52WK_SALES_AMT",
	"CAST(SPECIALTY_SALES AS DECIMAL(18,2)) as SPECIALTY_52WK_SALES_AMT",
	"CAST(COMP_CURR_FLAG as smallint) as COMP_CURR_FLAG",
	"CAST(SALES_CURR_FLAG as smallint) as SALES_CURR_FLAG",
	"CAST(LOCATION_TYPE_ID as smallint) as LOCATION_TYPE_ID"
)
try:
    # chk=DuplicateChecker()
    # chk.check_for_duplicate_primary_keys(Shortcut_to_SALES_RANKING_SALES_PRE,[key])
    Shortcut_to_SALES_RANKING_SALES_PRE.write.saveAsTable(f'{raw}.SALES_RANKING_SALES_PRE', mode = 'overwrite')
except Exception as e:
    raise e

# COMMAND ----------


