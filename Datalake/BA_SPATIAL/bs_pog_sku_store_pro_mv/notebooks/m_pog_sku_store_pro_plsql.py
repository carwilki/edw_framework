# Databricks notebook source
#Code converted on 2023-10-11 11:42:06
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

# Processing node ASQ_Shortcut_To_SKU_STORE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
INSERT OVERWRITE {legacy}.pog_sku_store_pro
SELECT
  pss.product_id,
  pss.location_id,
  pog_id,
  sku_capacity_qty,
  sku_facings_qty,
  sku_height_in,
  sku_depth_in,
  sku_width_in,
  unit_of_measure,
  tray_pack_nbr,
  pog_sku_status,
  pog_store_status,
  sku_capacity_qty * NVL(i.map_amt, 0) AS sku_capacity_cost
FROM (
  SELECT
    sk.product_id,
    st.location_id,
    sk.pog_id,
    sku_capacity_qty,
    sku_facings_qty,
    sku_height_in,
    sku_depth_in,
    sku_width_in,
    unit_of_measure,
    tray_pack_nbr,
    sk.pog_status AS pog_sku_status,
    st.pog_status AS pog_store_status
  FROM
    {legacy}.pog_sku_pro sk
  JOIN
    {legacy}.pog_store_pro st ON st.pog_id = sk.pog_id
  JOIN
    {legacy}.planogram_pro p ON st.pog_id = p.pog_id
  WHERE
    (
      (p.pog_type_cd = 'I' AND sk.pog_status <> 'D' AND st.pog_status <> 'D')
      OR
      (p.pog_type_cd = 'P' AND CURRENT_DATE BETWEEN sk.promo_start_dt AND sk.promo_end_dt)
    )
) pss
LEFT JOIN (
  SELECT
    product_id,
    location_id,
    map_amt
  FROM
    {legacy}.inv_instock_price_day
  WHERE
    day_dt = CURRENT_DATE - INTERVAL 1 DAY
) i
ON
  pss.product_id = i.product_id
  AND pss.location_id = i.location_id;
"""

spark.sql(_sql)
