# Databricks notebook source
#Code converted on 2023-10-11 11:42:45
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

# Processing node ASQ_Shortcut_To_SALES_WEEK_SKU_STORE, type SOURCE 
# COLUMN COUNT: 18

_sql = f"""
SELECT
    sdss.week_dt AS week_dt,
    sdss.product_id AS product_id,
    sdss.location_id AS location_id,
    SUM(sdss.sales_amt) AS sales_amt,
    SUM(sdss.sales_cost) AS sales_cost,
    SUM(sdss.sales_qty) AS sales_qty,
    SUM(sdss.return_amt) AS return_amt,
    SUM(sdss.return_cost) AS return_cost,
    SUM(sdss.return_qty) AS return_qty,
    SUM(sdss.discount_amt) AS discount_amt,
    SUM(sdss.discount_return_amt) AS discount_return_amt,
    SUM(sdss.pos_coupon_amt) AS pos_coupon_amt,
    SUM(sdss.special_sales_amt) AS special_sales_amt,
    SUM(sdss.special_sales_qty) AS special_sales_qty,
    SUM(sdss.special_return_amt) AS special_return_amt,
    SUM(sdss.special_return_qty) AS special_return_qty,
    MAX(NVL(pmdwp.merch_discount_amt, 0)) AS merch_discount_amt,
    MAX(NVL(pmdwp.merch_discount_return_amt, 0)) AS merch_discount_return_amt
FROM
    {legacy}.sales_day_sku_store sdss
LEFT OUTER JOIN
    {raw}.pog_merch_discount_week_pre pmdwp
ON
    sdss.week_dt = pmdwp.week_dt
    AND sdss.product_id = pmdwp.product_id
    AND sdss.location_id = pmdwp.location_id
WHERE
    sdss.week_dt = DATE_ADD(CURRENT_DATE, -(date_part('dow', CURRENT_DATE) - 1))
    AND NOT EXISTS (
        SELECT DISTINCT
            pswssp.week_dt,
            pswssp.product_id,
            pswssp.location_id
        FROM
            {raw}.pog_sales_week_sku_store_pre pswssp
        WHERE
            pswssp.week_dt = DATE_ADD(CURRENT_DATE, -(date_part('dow', CURRENT_DATE) - 1))
            AND pswssp.product_id = sdss.product_id
            AND pswssp.location_id = sdss.location_id
    )
GROUP BY
    sdss.week_dt,
    sdss.product_id,
    sdss.location_id
"""


ASQ_Shortcut_To_SALES_WEEK_SKU_STORE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_SALES_WEEK_SKU_STORE = ASQ_Shortcut_To_SALES_WEEK_SKU_STORE \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[0],'WEEK_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[2],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[3],'SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[4],'SALES_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[5],'SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[6],'RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[7],'RETURN_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[8],'RETURN_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[9],'DISCOUNT_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[10],'DISCOUNT_RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[11],'POS_COUPON_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[12],'SPECIAL_SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[13],'SPECIAL_SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[14],'SPECIAL_RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[15],'SPECIAL_RETURN_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[16],'MERCH_DISCOUNT_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_WEEK_SKU_STORE.columns[17],'MERCH_DISCOUNT_RETURN_AMT')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

EXPTRANS = ASQ_Shortcut_To_SALES_WEEK_SKU_STORE \
    .withColumn("POG_ID", expr("9999999")) \
    .withColumn("NET_SALES_AMT", expr("( SALES_AMT - RETURN_AMT - ( DISCOUNT_AMT - DISCOUNT_RETURN_AMT ) - ( SPECIAL_SALES_AMT - SPECIAL_RETURN_AMT ) - POS_COUPON_AMT )")) \
    .withColumn("NET_SALES_COST", expr("SALES_COST - RETURN_COST")) \
    .withColumn("MERCH_SALES_AMT", expr("( SALES_AMT - RETURN_AMT - ( MERCH_DISCOUNT_AMT - MERCH_DISCOUNT_RETURN_AMT ) - ( SPECIAL_SALES_AMT - SPECIAL_RETURN_AMT ) - POS_COUPON_AMT )")) \
    .withColumn("NET_SALES_QTY", expr(" (SALES_QTY - RETURN_QTY - ( SPECIAL_SALES_QTY - SPECIAL_RETURN_QTY))")) \
    .withColumn("POG_TYPE_CD", expr("'D'")) \
    .withColumn("POG_MULTI_INLINE_FLAG", expr("'N'")) \
    .withColumn("POG_MULTI_PLANNER_FLAG", expr("'N'")) \
    .withColumn("PRODLOC_PROMO_QTY", expr("0")) \
    .withColumn("INLINE_CNT", expr("1")) \
    .withColumn("PLANNER_CNT", expr("0"))
    


# COMMAND ----------

# Processing node Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE, type TARGET 
# COLUMN COUNT: 14


Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE = EXPTRANS.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_ID AS INT) as POG_ID",
	"CAST(NET_SALES_AMT AS DECIMAL(8,2)) as NET_SALES_AMT",
	"CAST(NET_SALES_COST AS DECIMAL(8,2)) as NET_SALES_COST",
	"CAST(NET_SALES_QTY AS DECIMAL(10,3)) as NET_SALES_QTY",
	"CAST(MERCH_SALES_AMT AS DECIMAL(8,2)) as MERCH_SALES_AMT",
	"CAST(POG_TYPE_CD AS STRING) as POG_TYPE_CD",
	"CAST(POG_MULTI_INLINE_FLAG AS STRING) as POG_MULTI_INLINE_FLAG",
	"CAST(POG_MULTI_PLANNER_FLAG AS STRING) as POG_MULTI_PLANNER_FLAG",
	"CAST(PRODLOC_PROMO_QTY AS INT) as PRODLOC_PROMO_QTY",
	"CAST(INLINE_CNT AS INT) as INLINE_CNT",
	"CAST(PLANNER_CNT AS INT) as PLANNER_CNT"
)


Shortcut_To_POG_SALES_WEEK_SKU_STORE_PRE.write.mode("append").saveAsTable(f'{raw}.POG_SALES_WEEK_SKU_STORE_PRE')

# COMMAND ----------


