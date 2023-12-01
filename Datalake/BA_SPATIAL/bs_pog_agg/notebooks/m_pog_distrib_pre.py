# Databricks notebook source
#Code converted on 2023-10-11 11:42:44
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import has_duplicates
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

# Processing node ASQ_Shortcut_To_PLANOGRAM_PRO, type SOURCE 
# COLUMN COUNT: 11

_sql = f"""
select PRODUCT_ID,LOCATION_ID, POG_ID,POG_TYPE_CD,inline_cnt, planner_cnt, prodloc_cnt, pog_promo_qty, promo_start_dt, promo_end_dt, prodloc_promo_qty  from (
SELECT pog_sku_prop.product_id AS PRODUCT_ID,
     pog_store_prop.location_id AS LOCATION_ID,
        planogram_pro.pog_id AS POG_ID,
     planogram_pro.pog_type_cd AS POG_TYPE_CD ,
         SUM (DECODE (planogram_pro.pog_type_cd,'I', 1, 0)) OVER (PARTITION BY pog_sku_prop.product_id, pog_store_prop.location_id)   AS inline_cnt,
         SUM (DECODE (planogram_pro.pog_type_cd, 'P', 1, 0)) OVER (PARTITION BY pog_sku_prop.product_id, pog_store_prop.location_id)   AS planner_cnt,
         COUNT (*) OVER (PARTITION BY pog_sku_prop.product_id, pog_store_prop.location_id) AS prodloc_cnt,
        pog_sku_prop.pog_promo_qty AS pog_promo_qty  ,
     pog_sku_prop.promo_start_dt AS promo_start_dt,
        pog_sku_prop.promo_end_dt AS promo_end_dt   ,
         SUM (pog_sku_prop.pog_promo_qty) OVER (PARTITION BY pog_sku_prop.product_id, pog_store_prop.location_id)    AS prodloc_promo_qty
FROM {legacy}.planogram_pro,
        (
SELECT  a.product_id AS product_id,a.pog_id AS pog_id,  a.pog_promo_qty AS pog_promo_qty,  a.promo_start_dt AS promo_start_dt,  a.promo_end_dt AS promo_end_dt
FROM {legacy}.pog_sku_pro a,  {legacy}.planogram_pro b
WHERE b.pog_id = a.pog_id
  AND ( (  b.pog_type_cd = 'I'
  AND a.date_pog_added <(  CURRENT_DATE - (date_part ('dow', CURRENT_DATE) - 1))- 1 AND a.date_pog_deleted >=(CURRENT_DATE - (date_part ('dow', CURRENT_DATE) - 1))- 1)
  OR (b.pog_type_cd = 'P' AND a.promo_start_dt <(CURRENT_DATE - (date_part ('dow', CURRENT_DATE) - 1))- 1 AND a.promo_end_dt >=(CURRENT_DATE - (date_part ('dow', CURRENT_DATE) - 1))- 1))) pog_sku_prop,
        (
SELECT c.location_id AS location_id,c.pog_id AS pog_id
FROM {legacy}.pog_store_pro c,     {legacy}.planogram_pro d
WHERE d.pog_id = c.pog_id
    AND (d.pog_type_cd = 'P' OR (d.pog_type_cd = 'I' AND c.date_pog_added <(CURRENT_DATE - (date_part ('dow', CURRENT_DATE) - 1)  )- 1 AND c.date_pog_deleted >= (CURRENT_DATE - (date_part ('dow', CURRENT_DATE) - 1)) - 1))
	) pog_store_prop
WHERE planogram_pro.pog_id = pog_sku_prop.pog_id
    AND planogram_pro.pog_id = pog_store_prop.pog_id
GROUP BY pog_sku_prop.product_id,
                        pog_store_prop.location_id,
                        planogram_pro.pog_id,
                        planogram_pro.pog_type_cd,
                        pog_sku_prop.pog_promo_qty,
                        pog_sku_prop.promo_start_dt,
                        pog_sku_prop.promo_end_dt
) temp
"""

ASQ_Shortcut_To_PLANOGRAM_PRO = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_PLANOGRAM_PRO = ASQ_Shortcut_To_PLANOGRAM_PRO \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[1],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[2],'POG_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[3],'POG_TYPE_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[4],'INLINE_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[5],'PLANNER_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[6],'PRODLOC_CNT') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[7],'POG_PROMO_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[8],'PROMO_START_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[9],'PROMO_END_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_PLANOGRAM_PRO.columns[10],'PRODLOC_PROMO_QTY')


# COMMAND ----------

# Processing node Shortcut_To_POG_DISTRIB_PRE, type TARGET 
# COLUMN COUNT: 11


Shortcut_To_POG_DISTRIB_PRE = ASQ_Shortcut_To_PLANOGRAM_PRO.selectExpr(
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_ID AS INT) as POG_ID",
	"CAST(POG_TYPE_CD AS STRING) as POG_TYPE_CD",
	"CAST(INLINE_CNT AS INT) as INLINE_CNT",
	"CAST(PLANNER_CNT AS INT) as PLANNER_CNT",
	"CAST(PRODLOC_CNT AS INT) as PRODLOC_CNT",
	"CAST(POG_PROMO_QTY AS INT) as POG_PROMO_QTY",
	"CAST(PROMO_START_DT AS TIMESTAMP) as PROMO_START_DT",
	"CAST(PROMO_END_DT AS TIMESTAMP) as PROMO_END_DT",
	"CAST(PRODLOC_PROMO_QTY AS INT) as PRODLOC_PROMO_QTY"
)

if has_duplicates(Shortcut_To_POG_DISTRIB_PRE, ["PRODUCT_ID", "LOCATION_ID", "POG_ID"]):
    raise Exception("Duplicates found in the dataset")

Shortcut_To_POG_DISTRIB_PRE.write.mode("overwrite").saveAsTable(f'{raw}.POG_DISTRIB_PRE')

# COMMAND ----------


