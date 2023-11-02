# Databricks notebook source
#Code converted on 2023-09-15 14:55:15
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

# # Processing node SQ_Shortcut_To_INVENTORY_WEEK, type SOURCE 
# # COLUMN COUNT: 2

# _sql = f"""
# INSERT INTO {legacy}.inv_instock_price_day
# SELECT day_dt, iid.product_id, iid.location_id, from_location_id,
#     source_vendor_id, status_id AS sku_status_id,
#     site_sales_flag AS store_open_ind, out_of_stock_ind, pog_listed_ind,
#     sap_listed_ind, inline_ind, planner_ind, subs_ind, map_amt,
#     CASE
#      WHEN TRIM (country_cd) = 'CA'
#         THEN c.exchange_rate_pcnt
#      WHEN TRIM (country_cd) = 'US'
#         THEN 1
#     END AS exchange_rate_pct,
#     on_hand_qty, NVL(committed_qty, 0) AS committed_qty, xfer_in_trans_qty, on_order_qty,
#     sum_cost, bum_cost, on_order_cd,
#     CASE
#      WHEN retail_price_amt > 0
#      AND TRIM (country_cd) = 'US'
#         THEN retail_price_amt
#      WHEN retail_price_amt > 0 AND TRIM (country_cd) = 'CA'
#         THEN ROUND (retail_price_amt / c.exchange_rate_pcnt, 2)
#      WHEN ((TRIM (country_cd) = 'US') and (company_id <> 1100) )
#         THEN NVL (nat_price_us_amt, nat_price_ca_amt)
#      WHEN ((TRIM (country_cd) = 'US') and (company_id = 1100) )
#         THEN NVL (nat_price_pr_amt, nat_price_ca_amt)
#      WHEN TRIM (country_cd) = 'CA'
#         THEN ROUND (  NVL (nat_price_ca_amt, nat_price_us_amt)
#                     / c.exchange_rate_pcnt,
#                     2
#                    )
#      ELSE 0
#     END AS retail_price_amt,
#     sku_facings_qty, sku_capacity_qty, petsperk_amt, petsperk_ind,
#     local_price_amt, loc_petperks_price_amt, CURRENT_DATE() AS load_dt
# FROM (SELECT exchange_rate_pcnt
#    FROM {legacy}.currency_day
#    WHERE day_dt = DATE_ADD(to_date('2023-10-28','yyyy-MM-dd'), -1)) c
# JOIN (SELECT iip.day_dt, iip.product_id, iip.location_id,
#                   sku.nat_price_us_amt, sku.nat_price_ca_amt, sku.nat_price_pr_amt, sku.status_id,
#                   NVL (sc.from_location_id, 90000) AS from_location_id,
#                   NVL (sc.source_vendor_id, 1802) AS source_vendor_id,
#                   sp.site_sales_flag,sp.company_id, NVL (avg_qty, 0) avg_qty,
#                   NVL (out_of_stock_ind, 0) out_of_stock_ind,
#                   NVL (pog_listed_ind, 0) pog_listed_ind,
#                   NVL (sap_listed_ind, 0) sap_listed_ind,
#                   NVL (inline_ind, 0) inline_ind,
#                   NVL (planner_ind, 0) planner_ind, NVL (subs_ind,
#                                                          0) subs_ind,
#                   NVL (idp.map_amt, 0) map_amt,
#                   NVL (on_hand_qty, 0) on_hand_qty,
#                   NVL (committed_qty, 0) committed_qty,
#                   NVL (xfer_in_trans_qty, 0) xfer_in_trans_qty,
#                   NVL (on_order_qty, 0) on_order_qty,
#                   NVL (sum_cost, 0) sum_cost, NVL (bum_cost, 0) bum_cost,
#                   NVL (on_order_cd, ' ') on_order_cd,
#                   NVL (pd.retail_price_amt, 0) retail_price_amt,
#                   NVL (pd.local_price_amt, 0) local_price_amt,
#                   NVL (sku_capacity_qty, 0) sku_capacity_qty,
#                   NVL (petsperk_amt, 0) petsperk_amt,
#                   CASE
#                      WHEN NVL (petsperk_amt, 0) > 0
#                         THEN 1
#                      ELSE 0
#                   END AS petsperk_ind, country_cd, loc_petperks_price_amt,
#                   sku_facings_qty
#              FROM {raw}.inv_instock_pre iip
#              LEFT OUTER JOIN {raw}.inventory_day_pre idp ON iip.day_dt = DATE_ADD(to_date('2023-10-28','yyyy-MM-dd'), -1)
#                                                       AND iip.product_id = idp.product_id
#                                                       AND iip.location_id = idp.location_id
#              LEFT OUTER JOIN (SELECT product_id, location_id,
#                                       CASE
#                                          WHEN ad_price_amt >
#                                                     0
#                                             THEN ad_price_amt
#                                          WHEN regular_price_amt >
#                                                     0
#                                             THEN regular_price_amt
#                                          WHEN nat_price_amt >
#                                                     0
#                                             THEN nat_price_amt
#                                       END AS retail_price_amt,
#                                       petperks_price_amt,
#                                       CASE
#                                          WHEN loc_ad_price_amt >
#                                                      0
#                                             THEN loc_ad_price_amt
#                                          WHEN loc_regular_price_amt >
#                                                      0
#                                             THEN loc_regular_price_amt
#                                          WHEN loc_nat_price_amt >
#                                                      0
#                                             THEN loc_nat_price_amt
#                                       END AS local_price_amt,
#                                       loc_petperks_price_amt
#                              FROM {legacy}.sku_site_profile ssp) pd ON iip.product_id = pd.product_id
#                                                                    AND iip.location_id = pd.location_id
#              LEFT OUTER JOIN {legacy}.supply_chain sc ON iip.location_id = sc.location_id
#                                                      AND iip.product_id = sc.product_id
#              CROSS JOIN
#                   (SELECT product_id, sku_nbr, nat_price_us_amt,
#                           nat_price_ca_amt, nat_price_pr_amt, status_id
#                      FROM {legacy}.sku_profile) sku
#              JOIN (SELECT location_id, TRIM (country_cd) AS country_cd,
#                           site_sales_flag, company_id
#                    FROM {legacy}.site_profile
#                    WHERE store_type_id = '120') sp ON iip.location_id = sp.location_id
#             WHERE (   on_hand_qty <> 0
#                    OR on_order_qty <> 0
#                    OR xfer_in_trans_qty <> 0
#                    OR committed_qty <> 0
#                    OR iip.pog_listed_ind = 1
#                   )) iid
# """

# spark.sql(_sql)

# COMMAND ----------

_sql = f"""INSERT INTO {legacy}.inv_instock_price_day
   SELECT day_dt, iid.product_id, iid.location_id, from_location_id,
          source_vendor_id, status_id AS sku_status_id,
          site_sales_flag AS store_open_ind, out_of_stock_ind, pog_listed_ind,
          sap_listed_ind, inline_ind, planner_ind, subs_ind, map_amt,
          CASE
             WHEN TRIM (country_cd) = 'CA'
                THEN c.exchange_rate_pcnt
             WHEN TRIM (country_cd) = 'US'
                THEN 1
          END AS exchange_rate_pct,
          on_hand_qty, committed_qty, xfer_in_trans_qty, on_order_qty,
          sum_cost, bum_cost, on_order_cd,
          CASE
             WHEN retail_price_amt > 0
             AND TRIM (country_cd) = 'US'
                THEN retail_price_amt
             WHEN retail_price_amt > 0 AND TRIM (country_cd) = 'CA'
                THEN ROUND (retail_price_amt / c.exchange_rate_pcnt, 2)
             WHEN ((TRIM (country_cd) = 'US') and (company_id <> 1100) )
                THEN NVL (nat_price_us_amt, nat_price_ca_amt)
             WHEN ((TRIM (country_cd) = 'US') and (company_id = 1100) )
                THEN NVL (nat_price_pr_amt, nat_price_ca_amt)
             WHEN TRIM (country_cd) = 'CA'
                THEN ROUND (  NVL (nat_price_ca_amt, nat_price_us_amt)
                            / c.exchange_rate_pcnt,
                            2
                           )
             ELSE 0
          END AS retail_price_amt,
          sku_facings_qty, sku_capacity_qty, petsperk_amt, petsperk_ind,
          local_price_amt, loc_petperks_price_amt, CURRENT_DATE AS load_dt
     FROM (SELECT exchange_rate_pcnt
             FROM {legacy}.currency_day
            WHERE day_dt =  DATE_ADD(CURRENT_DATE(), -1)) c,
          (SELECT iip.day_dt, iip.product_id, iip.location_id,
                  sku.nat_price_us_amt, sku.nat_price_ca_amt, sku.nat_price_pr_amt ,sku.status_id,
                  NVL (sc.from_location_id, 90000) AS from_location_id,
                  NVL (sc.source_vendor_id, 1802) AS source_vendor_id,
                  sp.site_sales_flag,sp.company_id, NVL (avg_qty, 0) avg_qty,
                  NVL (out_of_stock_ind, 0) out_of_stock_ind,
                  NVL (pog_listed_ind, 0) pog_listed_ind,
                  NVL (sap_listed_ind, 0) sap_listed_ind,
                  NVL (inline_ind, 0) inline_ind,
                  NVL (planner_ind, 0) planner_ind, NVL (subs_ind,
                                                         0) subs_ind,
                  NVL (idp.map_amt, 0) map_amt,
                  NVL (on_hand_qty, 0) on_hand_qty,
                  NVL (committed_qty, 0) committed_qty,
                  NVL (xfer_in_trans_qty, 0) xfer_in_trans_qty,
                  NVL (on_order_qty, 0) on_order_qty,
                  NVL (sum_cost, 0) sum_cost, NVL (bum_cost, 0) bum_cost,
                  NVL (on_order_cd, ' ') on_order_cd,
                  NVL (pd.retail_price_amt, 0) retail_price_amt,
                  NVL (pd.local_price_amt, 0) local_price_amt,
                  NVL (sku_capacity_qty, 0) sku_capacity_qty,
                  NVL (petsperk_amt, 0) petsperk_amt,
                  CASE
                     WHEN NVL (petsperk_amt, 0) > 0
                        THEN 1
                     ELSE 0
                  END AS petsperk_ind, country_cd, loc_petperks_price_amt,
                  sku_facings_qty
             FROM {raw}.inv_instock_pre iip LEFT OUTER JOIN {raw}.inventory_day_pre idp ON iip.day_dt =
                                                                                 idp.day_dt
                                                                          AND iip.product_id =
                                                                                 idp.product_id
                                                                          AND iip.location_id =
                                                                                 idp.location_id
                  LEFT OUTER JOIN (SELECT product_id, location_id,
                                          CASE
                                             WHEN ad_price_amt >
                                                        0
                                                THEN ad_price_amt
                                             WHEN regular_price_amt >
                                                        0
                                                THEN regular_price_amt
                                             WHEN nat_price_amt >
                                                        0
                                                THEN nat_price_amt
                                          END AS retail_price_amt,
                                          petperks_price_amt,
                                          CASE
                                             WHEN loc_ad_price_amt >
                                                         0
                                                THEN loc_ad_price_amt
                                             WHEN loc_regular_price_amt >
                                                         0
                                                THEN loc_regular_price_amt
                                             WHEN loc_nat_price_amt >
                                                         0
                                                THEN loc_nat_price_amt
                                          END AS local_price_amt,
                                          loc_petperks_price_amt
                                     FROM {legacy}.sku_site_profile ssp) pd ON iip.product_id =
                                                                        pd.product_id
                                                                 AND iip.location_id =
                                                                        pd.location_id
                  LEFT OUTER JOIN {legacy}.supply_chain sc ON iip.location_id =
                                                                sc.location_id
                                                AND iip.product_id =
                                                                 sc.product_id
                  ,
                  (SELECT product_id, sku_nbr, nat_price_us_amt,
                          nat_price_ca_amt, nat_price_pr_amt ,status_id
                     FROM {legacy}.sku_profile) sku,
                  (SELECT location_id, TRIM (country_cd) AS country_cd,
                          site_sales_flag, company_id
                     FROM {legacy}.site_profile
                    WHERE TRIM (store_type_id) = '120') sp
            WHERE iip.location_id = sp.location_id
              AND iip.product_id = sku.product_id
              AND (   on_hand_qty <> 0
                   OR on_order_qty <> 0
                   OR xfer_in_trans_qty <> 0
                   OR committed_qty <> 0
                   OR iip.pog_listed_ind = 1
                  )) iid
"""
spark.sql(_sql)

# COMMAND ----------


