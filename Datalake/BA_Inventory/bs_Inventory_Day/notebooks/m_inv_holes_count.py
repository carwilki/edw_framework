# Databricks notebook source
#Code converted on 2023-09-15 14:55:14
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

# Processing node SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY, type SOURCE 
# COLUMN COUNT: 10

_sql = f"""
SELECT
  DATE_ADD(i.day_dt, 1) AS day_dt,
  i.product_id,
  i.location_id,
  i.sku_status_id,
  0 AS counted_oos_ind,
  i.OUT_OF_STOCK_CNT AS out_of_stock_ind,
  i.INLINE_CNT AS inline_ind,
  i.POG_LISTED_IND,
  i.ON_HAND_QTY,
  DATE_ADD(i.load_dt, 1) AS load_dt
FROM (
  SELECT
    day_dt,
    product_id,
    location_id,
    sku_status_id,
    OUT_OF_STOCK_CNT,
    INLINE_CNT,
    POG_LISTED_IND,
    ON_HAND_QTY,
    load_dt
  FROM {legacy}.inv_instock_price_day
  WHERE
    day_dt = DATE_ADD(CURRENT_DATE, -2)
    AND inline_cnt = 1
    AND out_of_stock_cnt = 1
    AND SKU_STATUS_ID IN (21, 22, 23, 24, 37)
) i
LEFT JOIN (
  SELECT
    h.day_dt,
    s.product_id,
    si.location_id
  FROM {raw}.inv_holes_site_pre h
  JOIN {legacy}.sku_profile s ON h.SKU_NBR = s.sku_nbr
  JOIN {legacy}.site_profile si ON h.store_nbr = si.store_nbr
) hp ON i.product_id = hp.product_id AND i.location_id = hp.location_id
WHERE hp.product_id IS NULL

UNION

SELECT
  DATE_ADD(i.day_dt, 1) AS day_dt,
  i.product_id,
  i.location_id,
  i.sku_status_id,
  1 AS counted_oos_ind,
  i.OUT_OF_STOCK_CNT AS out_of_stock_ind,
  i.INLINE_CNT AS inline_ind,
  i.POG_LISTED_IND,
  i.ON_HAND_QTY,
  DATE_ADD(i.load_dt, 1) AS load_dt
FROM {raw}.inv_holes_site_pre h
JOIN {legacy}.inv_instock_price_day i
JOIN {legacy}.sku_profile s 
JOIN {legacy}.site_profile si ON i.location_id = si.location_id
 AND i.product_id = s.product_id
 AND i.day_dt = DATE_ADD(h.day_dt, -1) AND s.sku_nbr = h.sku_nbr AND si.store_nbr = h.store_nbr

UNION

SELECT
  hp.day_dt,
  hp.product_id,
  hp.location_id,
  hp.status_id AS sku_status_id,
  1 AS counted_oos_ind,
  0 AS out_of_stock_ind,
  0 AS inline_ind,
  0 AS POG_LISTED_IND,
  0 AS ON_HAND_QTY,
  CURRENT_DATE AS load_dt
FROM (
  SELECT
    h.day_dt,
    s.product_id,
    si.location_id,
    s.status_id,
    h.ON_HAND_QTY
  FROM {raw}.inv_holes_site_pre h
  JOIN {legacy}.sku_profile s ON h.SKU_NBR = s.sku_nbr
  JOIN {legacy}.site_profile si ON h.store_nbr = si.store_nbr
) hp
LEFT JOIN (
  SELECT
    day_dt,
    product_id,
    location_id
  FROM {legacy}.inv_instock_price_day
  WHERE day_dt = DATE_ADD(CURRENT_DATE, -2)
) i ON hp.product_id = i.product_id AND hp.location_id = i.location_id
WHERE i.product_id IS NULL
"""

SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Conforming fields names to the component layout
SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY = SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[3],'SKU_STATUS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[4],'COUNTED_OOS_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[5],'OUT_OF_STOCK_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[6],'INLINE_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[7],'POG_LISTED_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[8],'ON_HAND_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[9],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_INV_HOLES_COUNT, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_INV_HOLES_COUNT = SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(SKU_STATUS_ID AS STRING) as SKU_STATUS_ID",
	"CAST(COUNTED_OOS_IND AS TINYINT) as COUNTED_OOS_IND",
	"CAST(OUT_OF_STOCK_IND AS TINYINT) as OUT_OF_STOCK_IND",
	"CAST(INLINE_IND AS TINYINT) as INLINE_IND",
	"CAST(POG_LISTED_IND AS TINYINT) as POG_LISTED_IND",
	"CAST(ON_HAND_QTY AS INT) as ON_HAND_QTY",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
Shortcut_to_INV_HOLES_COUNT.write.mode("append").saveAsTable(f'{legacy}.INV_HOLES_COUNT')

# COMMAND ----------


