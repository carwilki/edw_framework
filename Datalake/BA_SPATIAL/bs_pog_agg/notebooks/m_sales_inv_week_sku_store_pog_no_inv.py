# Databricks notebook source
#Code converted on 2023-10-11 11:42:42
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
from Datalake.utils.pk.pk import DuplicateChecker

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
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1, type SOURCE 
# COLUMN COUNT: 11

_sql = f"""
WITH wk_start AS (
    SELECT DATEADD(CURRENT_DATE, -date_part('dow', CURRENT_DATE) + 1) AS week_start_date
)
SELECT P.WEEK_DT,
       P.PRODUCT_ID,
       P.LOCATION_ID,
       P.POG_ID,
       P.NET_SALES_AMT,
       P.NET_SALES_COST,
       P.NET_SALES_QTY,
       P.MERCH_SALES_AMT,
       P.POG_MULTI_INLINE_FLAG,
       P.POG_MULTI_PLANNER_FLAG,
       CASE
           WHEN trim(SP.COUNTRY_CD) = 'CA' THEN C.EXCHANGE_RATE_PCNT
           ELSE 1
       END AS EXCH_RATE_PCT
FROM {raw}.POG_SALES_WEEK_SKU_STORE_PRE P
LEFT OUTER JOIN (
    SELECT *
    FROM {legacy}.SALES_INV_WEEK_SKU_STORE_POG
    WHERE WEEK_DT = (SELECT week_start_date FROM wk_start)
) I
ON P.WEEK_DT = I.WEEK_DT
   AND P.PRODUCT_ID = I.PRODUCT_ID
   AND P.LOCATION_ID = I.LOCATION_ID
   AND P.POG_ID = I.POG_ID
LEFT JOIN {legacy}.CURRENCY_DAY C ON P.WEEK_DT = C.DAY_DT
LEFT JOIN {legacy}.SITE_PROFILE SP ON P.LOCATION_ID = SP.LOCATION_ID
WHERE C.DAY_DT = (SELECT week_start_date FROM wk_start)
   AND I.WEEK_DT IS NULL
"""

ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1 = spark.sql(_sql)

# Conforming fields names to the component layout
ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1 = ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1 \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[0],'WEEK_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[2],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[3],'POG_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[4],'NET_SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[5],'NET_SALES_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[6],'NET_SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[7],'MERCH_SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[8],'POG_MULTI_INLINE_FLAG') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[9],'POG_MULTI_PLANNER_FLAG') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[10],'EXCHANGE_RATE_PCNT')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 14


EXPTRANS = ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1 \
    .withColumn("POG_INLINE_PLANNER_FLAG", lit('N')) \
    .withColumn("ON_HAND_QTY", lit(0)) \
    .withColumn("ON_HAND_COST", lit(0))


# COMMAND ----------

# Processing node Shortcut_To_SALES_INV_WEEK_SKU_STORE_POG, type TARGET 
# COLUMN COUNT: 14


Shortcut_To_SALES_INV_WEEK_SKU_STORE_POG = EXPTRANS.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_ID AS INT) as POG_ID",
	"CAST(NET_SALES_AMT AS DECIMAL(8,2)) as NET_SALES_AMT",
	"CAST(NET_SALES_COST AS DECIMAL(8,2)) as NET_SALES_COST",
	"CAST(NET_SALES_QTY AS DECIMAL(10,3)) as NET_SALES_QTY",
	"CAST(MERCH_SALES_AMT AS DECIMAL(8,2)) as MERCH_SALES_AMT",
	"CAST(ON_HAND_QTY AS DECIMAL(10,3)) as ON_HAND_QTY",
	"CAST(ON_HAND_COST AS DECIMAL(8,2)) as ON_HAND_COST",
	"CAST(POG_INLINE_PLANNER_FLAG AS STRING) as POG_INLINE_PLANNER_FLAG",
	"CAST(POG_MULTI_INLINE_FLAG AS STRING) as POG_MULTI_INLINE_FLAG",
	"CAST(POG_MULTI_PLANNER_FLAG AS STRING) as POG_MULTI_PLANNER_FLAG",
	"CAST(EXCHANGE_RATE_PCNT AS DECIMAL(9,6)) as EXCH_RATE_PCT"
)

DuplicateChecker.check_for_duplicate_primary_keys(spark, f'{legacy}.SALES_INV_WEEK_SKU_STORE_POG', Shortcut_To_SALES_INV_WEEK_SKU_STORE_POG, ["WEEK_DT", "PRODUCT_ID", "LOCATION_ID", "POG_ID"])

Shortcut_To_SALES_INV_WEEK_SKU_STORE_POG.write.mode("append").saveAsTable(f'{legacy}.SALES_INV_WEEK_SKU_STORE_POG')

# COMMAND ----------


