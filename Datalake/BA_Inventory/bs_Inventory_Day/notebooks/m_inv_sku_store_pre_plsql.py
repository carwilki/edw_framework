# Databricks notebook source
#Code converted on 2023-09-15 14:55:25
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

# Processing node SQ_Shortcut_To_INVENTORY_PRE, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
INSERT OVERWRITE {raw}.inv_sku_store_pre
SELECT ID.product_id
    ,ID.location_id
    ,NVL(iss.first_on_hand_dt, ID.day_dt) AS first_on_hand_dt
    ,ID.day_dt AS last_on_hand_dt
    ,CURRENT_TIMESTAMP
FROM {legacy}.inv_instock_price_day ID
LEFT OUTER JOIN {legacy}.inv_sku_store iss ON ID.product_id = iss.product_id
    AND ID.location_id = iss.location_id
WHERE ID.day_dt = CURRENT_DATE - INTERVAL 1 DAY
    AND ID.on_hand_qty > 0

UNION

SELECT iss.product_id
    ,iss.location_id
    ,iss.first_on_hand_dt
    ,iss.last_on_hand_dt
    ,CURRENT_TIMESTAMP
FROM {legacy}.inv_sku_store iss
LEFT OUTER JOIN {legacy}.inv_instock_price_day ID ON iss.product_id = ID.product_id
    AND iss.location_id = ID.location_id
    AND ID.day_dt = CURRENT_DATE - INTERVAL 1 DAY
    AND ID.on_hand_qty > 0
WHERE ID.product_id IS NULL
"""

spark.sql(_sql)
