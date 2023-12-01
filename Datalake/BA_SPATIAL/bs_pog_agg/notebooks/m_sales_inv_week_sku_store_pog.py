# Databricks notebook source
#Code converted on 2023-10-11 11:42:46
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


# COMMAND ----------

# Processing node ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1, type SOURCE 
# COLUMN COUNT: 17

_sql = f"""
SELECT T.WEEK_DT AS WEEK_DT,
       T.PRODUCT_ID AS PRODUCT_ID,
       T.LOCATION_ID AS LOCATION_ID,
       nvl(P.POG_ID,9999999) AS POG_ID,
       nvl(P.NET_SALES_AMT,0) AS NET_SALES_AMT,
       nvl(P.NET_SALES_COST,0) AS NET_SALES_COST,
       nvl(P.NET_SALES_QTY,0) AS NET_SALES_QTY,
       nvl(P.MERCH_SALES_AMT,0) AS MERCH_SALES_AMT,
       nvl(P.POG_TYPE_CD,'D') AS POG_TYPE_CD,
       nvl(P.POG_MULTI_INLINE_FLAG,'N') AS POG_MULTI_INLINE_FLAG,
       nvl(P.POG_MULTI_PLANNER_FLAG,'N') AS POG_MULTI_PLANNER_FLAG,
       nvl(P.PRODLOC_PROMO_QTY,0) AS PRODLOC_PROMO_QTY,
       nvl(P.INLINE_CNT,0) AS INLINE_CNT,
       nvl(P.PLANNER_CNT,0) AS PLANNER_CNT,
       T.MAP_AMT AS MAP_AMT,
       T.ON_HAND_QTY AS ON_HAND_QTY,
       T.EXCH_RATE_PCT AS EXCHANGE_RATE_PCNT
FROM (
    SELECT I.WEEK_DT,
           I.PRODUCT_ID,
           I.LOCATION_ID,
           I.MAP_AMT,
           I.ON_HAND_QTY,
           I.EXCH_RATE_PCT
    FROM {legacy}.INV_INSTOCK_PRICE_WK I LEFT JOIN {legacy}.SITE_PROFILE S ON I.LOCATION_ID = S.LOCATION_ID
    WHERE I.WEEK_DT = DATE_ADD(CURRENT_DATE, - (DATE_PART('dow', CURRENT_DATE) - 1))
      AND trim(S.STORE_TYPE_ID) = '120'
) T
left outer JOIN {raw}.POG_SALES_WEEK_SKU_STORE_PRE P 
ON T.WEEK_DT = P.WEEK_DT
   AND T.PRODUCT_ID = P.PRODUCT_ID
   AND T.LOCATION_ID = P.LOCATION_ID
"""


ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1 = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
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
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[8],'POG_TYPE_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[9],'POG_MULTI_INLINE_FLAG') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[10],'POG_MULTI_PLANNER_FLAG') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[11],'PRODLOC_PROMO_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[12],'INLINE_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[13],'PLANNER_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[14],'MAP_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[15],'ON_HAND_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1.columns[16],'EXCHANGE_RATE_PCNT')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 14

EXPTRANS = ASQ_Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE1 \
    .withColumn("INV_ABOVE_PROMO_QTY", expr("IF(ON_HAND_QTY <= PRODLOC_PROMO_QTY, 0, CAST(ON_HAND_QTY - PRODLOC_PROMO_QTY AS INT))")) \
    .withColumn("MAX_WK_INV_QTY", expr("IF(ON_HAND_QTY <= PRODLOC_PROMO_QTY, ON_HAND_QTY, PRODLOC_PROMO_QTY)")) \
    .withColumn("DISTRIB_PRCNTG", expr("""
                                           CASE
                                               WHEN POG_TYPE_CD = 'D' THEN 1
                                               WHEN (ON_HAND_QTY = 0) THEN 0
                                               WHEN (INLINE_CNT >= 1) AND (PLANNER_CNT = 0) THEN 1 / INLINE_CNT
                                               WHEN (INLINE_CNT = 0) AND (PLANNER_CNT >= 1) THEN 1 / PLANNER_CNT
                                               WHEN (INLINE_CNT >= 1) AND (PLANNER_CNT >= 1) THEN
                                                   CASE
                                                       WHEN (POG_TYPE_CD = 'I') THEN 1 / INLINE_CNT * INV_ABOVE_PROMO_QTY / ON_HAND_QTY
                                                       WHEN (POG_TYPE_CD = 'P') THEN (1 / PLANNER_CNT) * MAX_WK_INV_QTY / ON_HAND_QTY
                                                   END
                                           END
                                       """)) \
    .withColumn("DISTRIB_ON_HAND_QTY_VAR", expr("ON_HAND_QTY * DISTRIB_PRCNTG")) \
    .withColumn("POG_INLINE_PLANNER_FLAG", expr("IF(INLINE_CNT > 0 AND PLANNER_CNT > 0, 'Y', 'N')")) \
    .withColumn("out_ON_HAND_QTY", expr("DISTRIB_ON_HAND_QTY_VAR")) \
    .withColumn("ON_HAND_COST", expr("DISTRIB_ON_HAND_QTY_VAR * MAP_AMT"))



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
	"CAST(out_ON_HAND_QTY AS DECIMAL(10,3)) as ON_HAND_QTY",
	"CAST(ON_HAND_COST AS DECIMAL(8,2)) as ON_HAND_COST",
	"CAST(POG_INLINE_PLANNER_FLAG AS STRING) as POG_INLINE_PLANNER_FLAG",
	"CAST(POG_MULTI_INLINE_FLAG AS STRING) as POG_MULTI_INLINE_FLAG",
	"CAST(POG_MULTI_PLANNER_FLAG AS STRING) as POG_MULTI_PLANNER_FLAG",
	"CAST(EXCHANGE_RATE_PCNT AS DECIMAL(9,6)) as EXCH_RATE_PCT"
)

DuplicateChecker.check_for_duplicate_primary_keys(spark, f'{legacy}.SALES_INV_WEEK_SKU_STORE_POG', Shortcut_To_SALES_INV_WEEK_SKU_STORE_POG, ["WEEK_DT", "PRODUCT_ID", "LOCATION_ID", "POG_ID"])

Shortcut_To_SALES_INV_WEEK_SKU_STORE_POG.write.mode("append").saveAsTable(f'{legacy}.SALES_INV_WEEK_SKU_STORE_POG')

# COMMAND ----------


