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

# Processing node ASQTRANS, type SOURCE 
# COLUMN COUNT: 4

_sql = f"""
SELECT sdss.product_id product_id,
       sdss.location_id location_id, 0 AS pog_id,
       SUM (sdss.sales_qty) / 4 AS avg_wk_sales_qty
FROM (SELECT product_id, location_id,
             MIN (promo_start_dt) promo_start_dt
      FROM {raw}.pog_distrib_pre
      WHERE inline_cnt > 0 AND planner_cnt > 0 AND pog_type_cd = 'P'
      GROUP BY product_id, location_id) pog_distrib_pr, {legacy}.sales_day_sku_store sdss
WHERE pog_distrib_pr.product_id = sdss.product_id
  AND pog_distrib_pr.location_id = sdss.location_id
  AND sdss.week_dt BETWEEN date_add(pog_distrib_pr.promo_start_dt, -21 - (date_part('dow', pog_distrib_pr.promo_start_dt - INTERVAL 21 DAYS) - 1))
                       AND date_add(pog_distrib_pr.promo_start_dt, -(date_part('dow', pog_distrib_pr.promo_start_dt) - 1))
GROUP BY sdss.product_id, sdss.location_id
ORDER BY sdss.product_id, sdss.location_id
"""

ASQTRANS = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQTRANS = ASQTRANS \
	.withColumnRenamed(ASQTRANS.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(ASQTRANS.columns[1],'LOCATION_ID') \
	.withColumnRenamed(ASQTRANS.columns[2],'POG_ID') \
	.withColumnRenamed(ASQTRANS.columns[3],'SALES_QTY')

# COMMAND ----------

# Processing node Shortcut_To_POG_AVG_WEEK_SALES_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_To_POG_AVG_WEEK_SALES_PRE = ASQTRANS.selectExpr(
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_ID AS INT) as POG_ID",
	"CAST(SALES_QTY AS INT) as AVG_WK_SALES_QTY"
)

# No duplicate check is needed due to the nature of the source query (group by)

Shortcut_To_POG_AVG_WEEK_SALES_PRE.write.mode("overwrite").saveAsTable(f'{raw}.POG_AVG_WEEK_SALES_PRE')

# COMMAND ----------


