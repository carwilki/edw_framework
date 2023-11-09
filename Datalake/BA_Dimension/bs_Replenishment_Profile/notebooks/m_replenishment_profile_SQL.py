# Databricks notebook source
#Code converted on 2023-09-26 15:05:22
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

"""
WARNING: SQL Transformation is not yet supported, producing passthrough dataframe:
SQL query:
INSERT INTO replenishment_profile   SELECT /*+ ORDERED  */          ssvd.product_id product_id, ssvd.location_id location_id,
          NVL (prfl.round_value_qty, 0) round_value_qty,          rep.safety_qty safety_qty, 
          rep.service_lvl_rt service_lvl_rt,          rep.reorder_point_qty reorder_point_qty,
          --          rep.plan_deliv_days plan_deliv_days,
          ddays.quote_lt_days PLAN_DELIV_DAYS,
          NVL (prfl.rounding_profile_cd, 'PC') round_profile_cd,          rep.target_stock_qty target_stock_qty,
          rep.present_qty present_qty,          pog.pog_capacity_qty pog_capacity_qty,
          pog.pog_facings_qty pog_facings_qty,
          /*CASE WHEN ssvd.store_type_id = '120'               THEN NVL (promo.max_promo_qty, 0)               ELSE NVL (promo.sum_promo_qty, 0)          END promo_qty,*/
          NULL promo_qty,          fcst.basic_value_qty basic_value_qty, fcst.last_fc_dt last_fc_dt,
          CURRENT_DATE load_dt-- ssvd
          FROM (SELECT DISTINCT v.sku_nbr, v.store_nbr, p.product_id,
                    l.location_id, l.store_type_id                 
                    FROM sku_store_vendor_day v, sku_profile p, site_profile l
                    WHERE v.delete_ind <> 'X'                  
                    AND v.sku_nbr = p.sku_nbr                  
                    AND v.store_nbr = l.store_nbr) ssvd-- prlf
                    LEFT OUTER JOIN 
                         (SELECT /*+ ORDERED  */                        svp.sku_nbr,                        svp.store_nbr,
                                  MAX(svp.vendor_id) vendor_id,                        
                                  MAX(rpp.rounding_profile_cd) rounding_profile_cd,                        
                                  MAX(su.uom_numerator / su.uom_denominator) round_value_qty                   
                              FROM sku_store_vendor_day svp, sku_vendor_day svd,  sku_profile sp,  rounding_profile_pre rpp,
                                   uom_rounding_rule_pre urrp,sku_uom su    
                              WHERE svp.sku_nbr = svd.sku_nbr                  
                              AND svp.vendor_id = svd.vendor_id                  
                              AND svd.rounding_profile_cd = rpp.rounding_profile_cd                  
                              AND rpp.rounding_rule_cd    = urrp.rounding_rule_cd                  
                              AND svp.sku_nbr = sp.sku_nbr                  
                              AND sp.product_id = su.product_id                  
                              AND urrp.uom_cd = su.uom_cd                  
                              AND TRIM(svp.delete_ind) = ''                  
                              AND TRIM(svd.delete_ind) = ''             
                              GROUP BY svp.sku_nbr, svp.store_nbr) prfl        
                         ON ssvd.sku_nbr = prfl.sku_nbr       
                              AND ssvd.store_nbr = prfl.store_nbr-- pog
                         LEFT OUTER JOIN (SELECT psku.product_id, psite.location_id,SUM(psku.sku_capacity_qty) pog_capacity_qty,
                                                 SUM(psku.sku_facings_qty) pog_facings_qty,
                                                 MAX(psku.last_chng_dt) pog_last_chng_dt                   
                                             FROM pog_sku_pro psku, pog_store_pro psite  
                                             WHERE psite.pog_id = psku.pog_id                    
                                             AND psku.date_pog_deleted > (CURRENT_DATE - 1)                    
                                             AND psite.date_pog_deleted > (CURRENT_DATE - 1)               
                                             GROUP BY psku.product_id, psite.location_id) pog    
                         ON ssvd.product_id = pog.product_id   
                              AND ssvd.location_id = pog.location_id-- rep
                         LEFT OUTER JOIN replenishment_day rep  ON ssvd.sku_nbr = rep.sku_nbr 
                              AND ssvd.store_nbr = rep.store_nbr-- fcst
                         LEFT OUTER JOIN (SELECT product_id, location_id, MAX (basic_value_qty) basic_value_qty,
                                                 MAX (last_fc_dt) last_fc_dt                   
                                             FROM dc_fcst_day               
                                             GROUP BY product_id, location_id) fcst  
                         ON ssvd.product_id = fcst.product_id 
                              AND ssvd.location_id = fcst.location_id-- ddays (pulling max lead time for multiple vendors)
                         LEFT OUTER JOIN (select s.product_id, s.location_id, v.quote_lt_days   
                                             from supply_chain s 
                                             JOIN (select location_id, vendor_id, max(quoted_lt_day_cnt) quote_lt_days 
                                                  from DP_SITE_VEND_PROFILE group by location_id, vendor_id) V                   
                              ON S.LOCATION_ID = V.LOCATION_ID    AND S.DIRECT_VENDOR_ID = V.VENDOR_ID) ddays  
                              on ssvd.product_id = ddays.product_id  and ssvd.location_id = ddays.location_id;-- promo/* --removed because ztb promo is retiredLEFT OUTER JOIN (SELECT sku_nbr, store_nbr,                        MIN (list_start_dt) promo_eff_dt,                        MAX (list_end_dt) promo_end_dt,                        SUM (promo_qty) sum_promo_qty,                        MAX (promo_qty) max_promo_qty                   FROM pog_ztb_promo                  WHERE CURRENT_TIMESTAMP BETWEEN repl_start_dt AND repl_end_dt                    AND TRIM (delete_ind) = ''               GROUP BY sku_nbr, store_nbr) promo  ON rep.sku_nbr = promo.sku_nbr AND rep.store_nbr = promo.store_nbr */
                              
                              
                              
"""
# for each involved DataFrame, append the dataframe name to each column
spark.sql(f"""
          
INSERT OVERWRITE {legacy}.replenishment_profile   
SELECT           ssvd.product_id product_id, ssvd.location_id location_id,
          NVL (prfl.round_value_qty, 0) round_value_qty,          rep.safety_qty safety_qty, 
          rep.service_lvl_rt service_lvl_rt,          rep.reorder_point_qty reorder_point_qty,
          --          rep.plan_deliv_days plan_deliv_days,
          ddays.quote_lt_days PLAN_DELIV_DAYS,
          NVL (prfl.rounding_profile_cd, 'PC') round_profile_cd,          rep.target_stock_qty target_stock_qty,
          rep.present_qty present_qty,          pog.pog_capacity_qty pog_capacity_qty,
          pog.pog_facings_qty pog_facings_qty,
          /*CASE WHEN ssvd.store_type_id = '120'               THEN NVL (promo.max_promo_qty, 0)               ELSE NVL (promo.sum_promo_qty, 0)          END promo_qty,*/
          NULL promo_qty,          fcst.basic_value_qty basic_value_qty, fcst.last_fc_dt last_fc_dt,
          CURRENT_DATE load_dt-- ssvd
          FROM (SELECT DISTINCT v.sku_nbr, v.store_nbr, p.product_id,
                    l.location_id, l.store_type_id                 
                    FROM {legacy}.sku_store_vendor_day v, {legacy}.sku_profile p, {legacy}.site_profile l
                    WHERE v.delete_ind <> 'X'                  
                    AND v.sku_nbr = p.sku_nbr                  
                    AND v.store_nbr = l.store_nbr) ssvd-- prlf
                    LEFT OUTER JOIN 
                         (SELECT                         svp.sku_nbr,                        svp.store_nbr,
                                  MAX(svp.vendor_id) vendor_id,                        
                                  MAX(rpp.rounding_profile_cd) rounding_profile_cd,                        
                                  MAX(su.uom_numerator / su.uom_denominator) round_value_qty                   
                              FROM {legacy}.sku_store_vendor_day svp, {legacy}.sku_vendor_day svd,  {legacy}.sku_profile sp,  {raw}.rounding_profile_pre rpp,
                                   {raw}.uom_rounding_rule_pre urrp,{legacy}.sku_uom su    
                              WHERE svp.sku_nbr = svd.sku_nbr                  
                              AND svp.vendor_id = svd.vendor_id                  
                              AND svd.rounding_profile_cd = rpp.rounding_profile_cd                  
                              AND rpp.rounding_rule_cd    = urrp.rounding_rule_cd                  
                              AND svp.sku_nbr = sp.sku_nbr                  
                              AND sp.product_id = su.product_id                  
                              AND urrp.uom_cd = su.uom_cd                  
                              AND TRIM(svp.delete_ind) = ''                  
                              AND TRIM(svd.delete_ind) = ''             
                              GROUP BY svp.sku_nbr, svp.store_nbr) prfl        
                         ON ssvd.sku_nbr = prfl.sku_nbr       
                              AND ssvd.store_nbr = prfl.store_nbr-- pog
                         LEFT OUTER JOIN (SELECT psku.product_id, psite.location_id,SUM(psku.sku_capacity_qty) pog_capacity_qty,
                                                 SUM(psku.sku_facings_qty) pog_facings_qty,
                                                 MAX(psku.last_chng_dt) pog_last_chng_dt                   
                                             FROM {legacy}.pog_sku_pro psku, {legacy}.pog_store_pro psite  
                                             WHERE psite.pog_id = psku.pog_id                    
                                             AND psku.date_pog_deleted > (CURRENT_DATE - 1)                    
                                             AND psite.date_pog_deleted > (CURRENT_DATE - 1)               
                                             GROUP BY psku.product_id, psite.location_id) pog    
                         ON ssvd.product_id = pog.product_id   
                              AND ssvd.location_id = pog.location_id-- rep
                         LEFT OUTER JOIN {legacy}.replenishment_day rep  ON ssvd.sku_nbr = rep.sku_nbr 
                              AND ssvd.store_nbr = rep.store_nbr-- fcst
                         LEFT OUTER JOIN (SELECT product_id, location_id, MAX (basic_value_qty) basic_value_qty,
                                                 MAX (last_fc_dt) last_fc_dt                   
                                             FROM {legacy}.dc_fcst_day               
                                             GROUP BY product_id, location_id) fcst  
                         ON ssvd.product_id = fcst.product_id 
                              AND ssvd.location_id = fcst.location_id-- ddays (pulling max lead time for multiple vendors)
                         LEFT OUTER JOIN (select s.product_id, s.location_id, v.quote_lt_days   
                                             from {legacy}.supply_chain s 
                                             JOIN (select location_id, vendor_id, max(quoted_lt_day_cnt) quote_lt_days 
                                                  from {legacy}.DP_SITE_VEND_PROFILE group by location_id, vendor_id) V                   
                              ON S.LOCATION_ID = V.LOCATION_ID    AND S.DIRECT_VENDOR_ID = V.VENDOR_ID) ddays  
                              on ssvd.product_id = ddays.product_id  and ssvd.location_id = ddays.location_id
          
          """)


# COMMAND ----------

# Processing node SQL_INS_to_SQL_TRANSFORM_LOG, type SQL_TRANSFORM 
# COLUMN COUNT: 14

"""
WARNING: SQL Transformation is not yet supported, producing passthrough dataframe:
SQL query:

SELECT COUNT(*) AS DUPLICATE_ROWS  FROM (SELECT PRODUCT_ID,               LOCATION_ID,               COUNT(*) AS CNT          FROM REPLENISHMENT_PROFILE         GROUP BY PRODUCT_ID, LOCATION_ID) T WHERE CNT > 1;

"""
# for each involved DataFrame, append the dataframe name to each column
spark.sql(f"""
      SELECT COUNT(*) AS DUPLICATE_ROWS  FROM (SELECT PRODUCT_ID,               LOCATION_ID,               COUNT(*) AS CNT          FROM {legacy}.REPLENISHMENT_PROFILE         GROUP BY PRODUCT_ID, LOCATION_ID) T WHERE CNT > 1;
          """)


# COMMAND ----------


