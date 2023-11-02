# Databricks notebook source
#Code converted on 2023-09-15 14:55:18
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

_sql = f"""INSERT OVERWRITE {raw}.inventory_day_pre 
SELECT inv.day_dt, prd.product_id, loc.location_id, inv.map_amt,
          inv.on_hand_qty, NVL (cq.committed_qty, 0) committed_qty,
          inv.xfer_in_trans_qty,
          CASE
             WHEN inv.xfer_in_trans_qty = 0
                THEN (NVL (po.order_qty, 0) + NVL (sto.order_qty, 0)
                     )
             WHEN NVL (sto.order_cnt, 0) > 1
                THEN (  NVL (po.order_qty, 0)
                      + NVL (sto.order_qty, 0)
                      - inv.xfer_in_trans_qty
                     )
             ELSE NVL (po.order_qty, 0)
          END AS on_order_qty,
          ROUND(CAST(CASE
             WHEN NVL (pcp.sum_cost, 0) > 0
                THEN pcp.sum_cost
             WHEN TRIM(loc.country_cd) = 'CA'
                THEN (prd.purch_cost_amt / c.exchange_rate_pcnt)
             ELSE prd.purch_cost_amt
          END as decimal(13,2))) AS sum_cost,
          ROUND(CAST(CASE
             WHEN NVL (pcp.bum_cost, 0) > 0
                THEN pcp.bum_cost
             WHEN TRIM(loc.country_cd) = 'CA'
                THEN (prd.purch_cost_amt / c.exchange_rate_pcnt)
             ELSE prd.purch_cost_amt
          END as decimal(13,2))) AS bum_cost,
          CASE
             WHEN NVL (pcp.retail_price_amt, 0) > 0
                THEN pcp.retail_price_amt
             WHEN ((prd.nat_price_us_amt > 0)
             AND (TRIM (loc.country_cd) = 'US') and (loc.company_id <> 1100) )
                THEN prd.nat_price_us_amt
             WHEN ( (prd.nat_price_pr_amt > 0)
             AND (TRIM (loc.country_cd) = 'US') and (loc.company_id = 1100) )
                THEN prd.nat_price_pr_amt

             WHEN prd.nat_price_ca_amt > 0 AND TRIM (loc.country_cd) = 'CA'
                THEN ROUND (prd.nat_price_ca_amt / c.exchange_rate_pcnt, 2)
          END AS retail_price_amt,
          NVL (petperks_amt, 0) petperks_amt,
          CASE
             WHEN po.order_cd = 'P' AND sto.order_cd = 'S'
                THEN 'B'
             WHEN po.order_cd = 'P'
                THEN 'P'
             WHEN sto.order_cd = 'S'
                THEN 'S'
             ELSE 'N'
          END AS on_order_cd,
          CURRENT_DATE AS load_dt
     FROM {legacy}.inventory_pre inv LEFT OUTER JOIN {raw}.sku_store_price_costs_pre pcp
          ON inv.sku_nbr = pcp.sku_nbr AND inv.store_nbr = pcp.store_nbr
          LEFT OUTER JOIN
          (SELECT   pp.sku_nbr, lp.store_nbr, SUM (opp.order_qty) order_qty,
                    'P' order_cd, COUNT (*) order_cnt
               FROM {raw}.open_po_pre opp, {legacy}.product pp, {legacy}.site_profile lp
              WHERE opp.product_id = pp.product_id
                AND opp.location_id = lp.location_id
           GROUP BY pp.sku_nbr, lp.store_nbr) po
          ON inv.sku_nbr = po.sku_nbr AND inv.store_nbr = po.store_nbr
          LEFT OUTER JOIN
          (SELECT   pp.sku_nbr, lp.store_nbr, SUM (osp.order_qty) order_qty,
                    'S' order_cd, COUNT (*) order_cnt
               FROM {raw}.open_sto_pre osp, {legacy}.product pp, {legacy}.site_profile lp
              WHERE osp.product_id = pp.product_id
                AND osp.location_id = lp.location_id
           GROUP BY pp.sku_nbr, lp.store_nbr) sto
          ON inv.sku_nbr = sto.sku_nbr AND inv.store_nbr = sto.store_nbr
          LEFT OUTER JOIN
          (SELECT   pp.sku_nbr, lp.store_nbr,
                    SUM
                       (CASE
                           WHEN osp.delivered_qty > 0
                              THEN 0
                           WHEN osp.issued_qty > 0
                              THEN osp.issued_qty
                           ELSE osp.order_qty
                        END
                       ) AS committed_qty
               FROM {raw}.open_sto_pre osp, {legacy}.product pp, {legacy}.site_profile lp
              WHERE osp.product_id = pp.product_id
                AND osp.supply_location_id = lp.location_id
           GROUP BY pp.sku_nbr, lp.store_nbr) cq
          ON inv.sku_nbr = cq.sku_nbr AND inv.store_nbr = cq.store_nbr
          ,
          (SELECT exchange_rate_pcnt
             FROM {legacy}.currency_day
            WHERE day_dt = current_date - INTERVAL 1 DAY) c,
          {legacy}.sku_profile prd,
          {legacy}.site_profile loc
    WHERE inv.store_nbr = loc.store_nbr
      AND inv.sku_nbr = prd.sku_nbr
      AND (   (inv.on_hand_qty <> 0)
           OR (inv.xfer_in_trans_qty <> 0)
           OR (NVL (po.order_qty, 0) <> 0)
           OR (NVL (sto.order_qty, 0) <> 0)
           OR (NVL (pcp.retail_price_amt, 0) <> 0)
          )"""
spark.sql(_sql)

# COMMAND ----------


